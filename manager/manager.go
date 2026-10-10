/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"gomodules.xyz/natjobs/tasks"

	cloudeventssdk "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"gomodules.xyz/wait"
	"k8s.io/klog/v2"
)

type Options struct {
	RequestTimeout time.Duration

	// AckWait is how long the server waits for an ack before it assumes the
	// worker is gone and redelivers the task.
	//
	// With Heartbeat enabled this means "how long may a worker be silent", not
	// "how long may a task take": a worker calls jetstream.Msg.InProgress()
	// every Heartbeat while its task runs, which resets this timer. That is
	// what makes an AckWait shorter than the longest task safe, and it is the
	// only thing that recovers a task whose worker died without running its
	// shutdown path -- SIGKILL after the grace period, OOM kill, node loss --
	// and so never got to NAK.
	AckWait time.Duration

	// Heartbeat is how often a worker tells the server it is still working on
	// its task. Zero disables heartbeats, in which case AckWait must exceed the
	// longest legitimate task duration or two workers end up running the same
	// task. It must stay well below AckWait; New clamps it if it does not.
	Heartbeat time.Duration

	// TaskTimeout bounds a single task function. It matters most when Heartbeat
	// is enabled: heartbeats renew the ack timer for as long as the task runs,
	// so without a deadline a wedged task is renewed forever and holds both its
	// worker and its MaxAckPending slot indefinitely. Zero means no deadline.
	TaskTimeout time.Duration

	// same as stream
	Stream string

	// manager id, < 0 means auto detect
	Id int
	// hostname
	Name string

	NumReplicas int
	NumWorkers  int

	// sends
	ResponseSubjectPrefix     string
	NotificationSubjectPrefix string

	LogNatsError bool
}

func DefaultOptions() Options {
	hostname, _ := os.Hostname()

	return Options{
		RequestTimeout: 5 * time.Second,
		// AckWait is deliberately much shorter than the longest task. Workers
		// heartbeat while they work, so this bounds only how long a task stays
		// stranded after its worker died without NAK'ing it. TaskTimeout keeps
		// the old 1h ceiling on a single task, which is what a 1h AckWait used
		// to enforce by redelivering.
		AckWait:                   2 * time.Minute,
		Heartbeat:                 15 * time.Second,
		TaskTimeout:               1 * time.Hour,
		Stream:                    "natjobs",
		Id:                        1,
		Name:                      hostname,
		NumReplicas:               1,
		NumWorkers:                1,
		ResponseSubjectPrefix:     "natjobs.resp",
		NotificationSubjectPrefix: "notifications",
		LogNatsError:              true,
	}
}

const (
	// maxDeliver is how many times the server delivers a task before it gives
	// up on it. There is no dead-letter stream, so the last failure response is
	// the only record a waiter will ever get.
	maxDeliver = 5

	// drainTimeout bounds the whole shutdown NAK pass. It has to stay well
	// inside terminationGracePeriodSeconds (30s by default), because a NAK that
	// has not gone out within a few seconds is not going out at all.
	drainTimeout = 3 * time.Second
	// nakRetryInterval is the pause between NAK attempts while the connection
	// still looks capable of carrying the publish.
	nakRetryInterval = 100 * time.Millisecond
)

// inflightMsg is a message one of this manager's workers currently owns.
type inflightMsg struct {
	msg jetstream.Msg
	// stop cancels the task and stops its heartbeat, returning only once the
	// heartbeat goroutine has exited so no InProgress can race the NAK that
	// follows. It is nil until the worker has built its task context.
	stop func()
}

type TaskManager struct {
	nc             *nats.Conn
	workerConsumer jetstream.Consumer
	requestTimeout time.Duration
	ackWait        time.Duration
	heartbeat      time.Duration
	taskTimeout    time.Duration

	// same as stream
	stream string

	// manager id, < 0 means auto detect
	id int
	// hostname
	name string

	numReplicas          int
	numWorkersPerReplica int

	// sends
	responseSubjectPrefix     string
	notificationSubjectPrefix string

	logNatsError bool

	// inflightMu guards inflight, nextClaim and draining.
	inflightMu sync.Mutex
	// inflight holds the messages currently being processed by this manager's
	// workers (at most one per worker), keyed by an opaque claim token. On
	// shutdown they are NAK'd so they are redelivered immediately instead of
	// after a full AckWait. A token (rather than the message value) is used as
	// the key so tracking never depends on the concrete message type being
	// comparable.
	inflight map[uint64]*inflightMsg
	// nextClaim is the source of monotonic claim tokens.
	nextClaim uint64
	// draining is set once the manager's context is cancelled, after which
	// workers stop claiming new messages.
	draining bool

	// drained is closed once shutdown has finished releasing in-flight
	// messages. Whoever owns the NATS connection waits on it before tearing the
	// connection down; see Drained.
	drained     chan struct{}
	drainedOnce sync.Once
}

func New(nc *nats.Conn, opts Options) *TaskManager {
	// A heartbeat that is not comfortably shorter than AckWait is worse than no
	// heartbeat at all: the server would redeliver tasks that healthy workers
	// are still running, so two workers would apply the same side effects.
	if opts.Heartbeat > 0 && opts.AckWait > 0 && opts.Heartbeat > opts.AckWait/2 {
		klog.Warningf("natjobs: heartbeat %v is too long for ack wait %v, using %v instead", opts.Heartbeat, opts.AckWait, opts.AckWait/3)
		opts.Heartbeat = opts.AckWait / 3
	}
	if opts.Heartbeat > 0 && opts.TaskTimeout <= 0 {
		klog.Warning("natjobs: heartbeats are enabled without a task timeout, so a wedged task will hold its worker slot forever")
	}

	return &TaskManager{
		nc:                        nc,
		requestTimeout:            opts.RequestTimeout,
		ackWait:                   opts.AckWait,
		heartbeat:                 opts.Heartbeat,
		taskTimeout:               opts.TaskTimeout,
		drained:                   make(chan struct{}),
		stream:                    opts.Stream,
		id:                        opts.Id,
		name:                      opts.Name,
		numReplicas:               opts.NumReplicas,
		numWorkersPerReplica:      opts.NumWorkers,
		responseSubjectPrefix:     opts.ResponseSubjectPrefix,
		notificationSubjectPrefix: opts.NotificationSubjectPrefix,
		logNatsError:              opts.LogNatsError,
	}
}

// claimMsg registers msg as in-flight so it can be NAK'd on shutdown and
// returns a token to release it with. ok is false if the manager is already
// draining, in which case the caller must not process the message.
func (mgr *TaskManager) claimMsg(msg jetstream.Msg) (token uint64, ok bool) {
	mgr.inflightMu.Lock()
	defer mgr.inflightMu.Unlock()

	if mgr.draining {
		return 0, false
	}
	if mgr.inflight == nil {
		mgr.inflight = map[uint64]*inflightMsg{}
	}
	mgr.nextClaim++
	mgr.inflight[mgr.nextClaim] = &inflightMsg{msg: msg}
	return mgr.nextClaim, true
}

// bindTaskStop attaches a task's stop func to an existing claim so shutdown can
// halt the task and its heartbeat before handing the message back. It reports
// false if shutdown already released the message, in which case the caller must
// not process it.
func (mgr *TaskManager) bindTaskStop(token uint64, stop func()) bool {
	mgr.inflightMu.Lock()
	defer mgr.inflightMu.Unlock()
	e, ok := mgr.inflight[token]
	if !ok {
		return false
	}
	e.stop = stop
	return true
}

// releaseMsg removes the claim from the in-flight set and reports whether the
// caller still owns the message (true). It returns false when shutdown already
// NAK'd the message, in which case the caller must not ack it.
func (mgr *TaskManager) releaseMsg(token uint64) bool {
	mgr.inflightMu.Lock()
	defer mgr.inflightMu.Unlock()
	if _, ok := mgr.inflight[token]; !ok {
		return false
	}
	delete(mgr.inflight, token)
	return true
}

// drainInflight marks the manager as draining and NAKs every in-flight message
// so it is redelivered immediately rather than after a full AckWait.
//
// A NAK is a publish, so this has to finish before whatever owns the NATS
// connection drains or closes it: a rejected NAK strands the task in-flight for
// the full AckWait and leaves nothing behind but a log line. Callers order the
// two by waiting on Drained.
func (mgr *TaskManager) drainInflight() {
	mgr.inflightMu.Lock()
	defer mgr.inflightMu.Unlock()
	mgr.draining = true

	deadline := time.Now().Add(drainTimeout)
	for token, e := range mgr.inflight {
		// Stop the task and its heartbeat first. Once the message is back with
		// the server it belongs to another worker, so this process must neither
		// keep renewing its ack timer nor keep applying its side effects.
		if e.stop != nil {
			e.stop()
		}
		if err := mgr.nakWithRetry(e.msg, deadline); err != nil {
			// Deliberately not gated on logNatsError: a dropped NAK presents as
			// a task that hangs for a full AckWait with no explanation, which is
			// expensive to diagnose from the outside.
			klog.ErrorS(err, "failed to NAK in-flight natjobs msg on shutdown, task stays in flight until AckWait expires",
				"id", e.msg.Headers().Get(nats.MsgIdHdr), "ackWait", mgr.ackWait)
		}
		delete(mgr.inflight, token)
	}
}

// nakWithRetry NAKs msg, retrying until deadline for as long as the connection
// could still carry the publish. A NAK lost to a transient connection state
// costs a full AckWait, which is worth a few hundred milliseconds of the
// shutdown budget to avoid.
func (mgr *TaskManager) nakWithRetry(msg jetstream.Msg, deadline time.Time) error {
	for {
		err := msg.Nak()
		if err == nil {
			return nil
		}
		// A closed or publish-draining connection will never accept the NAK, so
		// retrying only burns shutdown budget. Reconnecting is different: the
		// client buffers publishes and flushes them once it is back.
		if mgr.nc == nil || mgr.nc.IsClosed() || mgr.nc.IsDraining() ||
			!time.Now().Add(nakRetryInterval).Before(deadline) {
			return err
		}
		time.Sleep(nakRetryInterval)
	}
}

// Drained returns a channel that is closed once shutdown has finished handing
// in-flight messages back to the server, or has given up on them.
//
// Anything that tears down the NATS connection this manager was built on must
// wait on this first. A NAK is a publish, and a drained connection rejects
// publishes, so closing the connection concurrently with the drain silently
// loses every NAK and strands each in-flight task for a full AckWait.
//
// The channel is also closed when the manager never started workers -- Start
// returned an error, or ran with createConsumer=false -- so waiting on it is
// always safe.
func (mgr *TaskManager) Drained() <-chan struct{} {
	return mgr.drained
}

func (mgr *TaskManager) markDrained() {
	mgr.drainedOnce.Do(func() { close(mgr.drained) })
}

// newTaskContext builds the context handed to a task function. It is not
// derived from the manager's context, because an unrelated component shutting
// down must not kill a task mid-way; it is cancelled when the task's deadline
// passes or when shutdown hands this task back (see drainInflight).
func (mgr *TaskManager) newTaskContext() (context.Context, context.CancelFunc) {
	if mgr.taskTimeout > 0 {
		return context.WithTimeout(context.Background(), mgr.taskTimeout)
	}
	return context.WithCancel(context.Background())
}

// startHeartbeat tells the server every mgr.heartbeat that msg is still being
// worked on, which resets the server's redelivery timer. This is what makes an
// AckWait shorter than the longest task safe, and a short AckWait is the only
// thing that recovers a task whose worker died without NAK'ing it.
//
// The heartbeat stops when ctx is done, so a task that blows its deadline stops
// being renewed and the server takes it back instead of letting it hold a slot
// forever. The returned stop func waits for the goroutine to exit.
func (mgr *TaskManager) startHeartbeat(ctx context.Context, msg jetstream.Msg) func() {
	if mgr.heartbeat <= 0 {
		return func() {}
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(mgr.heartbeat)
		defer t.Stop()
		// failing keeps a run of failed heartbeats to a single log line: a NATS
		// outage that outlasts a long task would otherwise log every interval.
		failing := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				err := msg.InProgress()
				if err != nil && !failing && mgr.logNatsError {
					klog.ErrorS(err, "failed to report natjobs task in progress, task will be redelivered if this does not recover within AckWait",
						"id", msg.Headers().Get(nats.MsgIdHdr), "ackWait", mgr.ackWait)
				}
				failing = err != nil
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

// finalAttempt reports whether this delivery is the last one the server will
// make for the message, and which attempt it is.
func (mgr *TaskManager) finalAttempt(msg jetstream.Msg) (bool, uint64) {
	md, err := msg.Metadata()
	if err != nil {
		return false, 0
	}
	return md.NumDelivered >= maxDeliver, md.NumDelivered
}

func (mgr *TaskManager) Start(ctx context.Context, createConsumer bool) error {
	// Nothing will ever drain unless this call gets as far as launching workers,
	// so release Drained on every other path rather than hanging a shutdown
	// sequence that waits on it.
	workersStarted := false
	defer func() {
		if !workersStarted {
			mgr.markDrained()
		}
	}()

	jsm, err := jetstream.New(mgr.nc)
	if err != nil {
		return err
	}

	stream, err := jsm.Stream(ctx, mgr.stream)
	if stream == nil || err == jetstream.ErrStreamNotFound {
		_, err = jsm.CreateStream(ctx, jetstream.StreamConfig{
			Name:     mgr.stream,
			Subjects: []string{mgr.stream + ".queue.*"},
			// https://docs.nats.io/nats-concepts/core-nats/queue#stream-as-a-queue
			Retention:  jetstream.WorkQueuePolicy,
			MaxMsgs:    -1,
			MaxBytes:   -1,
			Discard:    jetstream.DiscardOld,
			MaxAge:     30 * 24 * time.Hour, // 30 days
			MaxMsgSize: 1 * 1024 * 1024,     // 1 MB
			Storage:    jetstream.FileStorage,
			Replicas:   1, // TODO: configure
			Duplicates: time.Hour,
		})
		if err != nil {
			return err
		}
	}

	// A submit-only caller (createConsumer=false, e.g. a short-lived setup job)
	// only needs the stream to exist so it can publish tasks. It must not create
	// the durable "workers" consumer or launch workers: if it fetched a task into
	// in-flight state and then exited before ack, redelivery would stall for a
	// full AckWait.
	if !createConsumer {
		return nil
	}

	// create nats consumer
	consumerName := "workers"
	consumer, err := jsm.CreateOrUpdateConsumer(ctx, mgr.stream, jetstream.ConsumerConfig{
		Name:      consumerName,
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   mgr.ackWait, // TODO: max for any task type
		// The number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored
		MaxWaiting: 1,
		// max working set
		MaxAckPending: mgr.numReplicas * mgr.numWorkersPerReplica,
		// one request per worker
		MaxRequestBatch: 1,
		// max_expires the max amount of time that a pull request with an expires should be allowed to remain active
		// MaxRequestExpires: 1 * time.Second,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    maxDeliver,
		FilterSubject: "",
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		return err
	}
	mgr.workerConsumer = consumer

	// On shutdown (context cancelled, e.g. the process receiving SIGTERM on
	// restart) negatively-acknowledge every message still in flight in this
	// process. NAK triggers immediate redelivery, so the next process picks the
	// task up right away instead of waiting a full AckWait for redelivery.
	//
	// Unlike deleting the shared durable consumer, this only affects the
	// messages this process holds, so other replicas still bound to the same
	// consumer keep working during a partial (e.g. rolling) restart.
	go func() {
		<-ctx.Done()
		mgr.drainInflight()
		mgr.markDrained()
	}()

	klog.Info("Starting workers")
	// Launch workers to process and proxy the message to relevant subject from nats subject
	for i := 0; i < mgr.numWorkersPerReplica; i++ {
		go wait.Until(func() { mgr.runWorker(ctx) }, 5*time.Second, ctx.Done())
	}
	workersStarted = true

	return nil
}

func (mgr *TaskManager) runWorker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := mgr.processNextMsg()
		if err != nil {
			if mgr.logNatsError && !(strings.Contains(err.Error(), nats.ErrTimeout.Error()) || strings.Contains(err.Error(), "nats: Exceeded MaxWaiting")) {
				klog.Errorln(err)
			}
			break
		}
	}
}

func (mgr *TaskManager) processNextMsg() (err error) {
	msg, err := mgr.workerConsumer.Next(jetstream.FetchMaxWait(time.Millisecond * 50))
	if err != nil || msg == nil {
		// no more msg to process
		err = errors.Wrap(err, "failed to fetch msg")
		return
	}

	// If the manager is shutting down, hand the message straight back so it is
	// redelivered immediately instead of being processed by a dying process.
	token, ok := mgr.claimMsg(msg)
	if !ok {
		if e := msg.Nak(); e != nil && mgr.logNatsError {
			klog.ErrorS(e, "failed to NAK natjobs msg during drain", "id", msg.Headers().Get(nats.MsgIdHdr))
		}
		return nil
	}

	// The task runs on its own bounded context. It is deliberately not derived
	// from the manager's context -- an unrelated component's shutdown must not
	// kill a task mid-way -- but it cannot be unbounded either, because the
	// heartbeat below renews the server's ack timer for as long as the task runs.
	taskCtx, cancelTask := mgr.newTaskContext()
	defer cancelTask()

	// Keep the ack timer alive while the task runs, so AckWait can be short
	// enough to recover from an ungraceful worker death without redelivering
	// tasks that are still legitimately running.
	stopHeartbeat := mgr.startHeartbeat(taskCtx, msg)

	// Give shutdown the means to stop this task and its heartbeat before it NAKs
	// the message.
	if !mgr.bindTaskStop(token, func() { cancelTask(); stopHeartbeat() }) {
		// Shutdown released the message while this worker was setting up.
		stopHeartbeat()
		return nil
	}

	var ev *cloudeventssdk.Event

	defer func() {
		// Stop renewing the ack timer before deciding the message's fate.
		stopHeartbeat()

		// Report and ack only if this worker still owns the message. If shutdown
		// handed it back while the task was running, another worker is running
		// the task now: a terminal status here would tell waiters the task
		// failed when it has only moved, and the ack would ack a message this
		// process no longer owns.
		if !mgr.releaseMsg(token) {
			return
		}

		if ev != nil {
			var respMsg string
			var status TaskStatus
			if err != nil {
				respMsg = getTitle(*ev) + " failed!"
				if final, attempt := mgr.finalAttempt(msg); final {
					// There is no dead-letter stream, so past maxDeliver the
					// server drops the task silently. Say so here, or the waiter
					// just times out with no record of why.
					respMsg = fmt.Sprintf("%s (attempt %d of %d, giving up)", respMsg, attempt, maxDeliver)
				}
				status = TaskStatusFailed
			} else {
				respMsg = getTitle(*ev) + " completed successfully!"
				status = TaskStatusSuccess
			}
			if mgr.sendUpdates(*ev) {
				mgr.mustPublish(mgr.respSubject(*ev), mgr.newResponse(status, ev.ID(), "", respMsg, err))
			}
			if mgr.sendNotification(*ev) {
				mgr.mustPublish(mgr.notificationSubj(*ev), mgr.newResponse(status, ev.ID(), "", respMsg, err))
			}
		}

		// report failure ?
		if e2 := msg.Ack(); e2 != nil && mgr.logNatsError {
			klog.ErrorS(e2, "failed ACK msg", "id", msg.Headers().Get(nats.MsgIdHdr))
		}
	}()

	newEvent := cloudeventssdk.NewEvent()
	err = newEvent.UnmarshalJSON(msg.Data())
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal event")
	}
	ev = &newEvent

	def, ok := tasks.Get(tasks.TaskType(ev.Type()))
	if !ok {
		return errors.Errorf("No TaskDef registered for task type %s", ev.Type())
	}

	loggerOpts := funcr.Options{}
	if def.RespLoggerOpts() != nil {
		loggerOpts = *def.RespLoggerOpts()
	}

	ctx := taskCtx
	if mgr.sendUpdates(*ev) {
		logger := funcr.NewJSON(func(obj string) {
			data := mgr.logResponse(ev.ID(), obj)
			if err := mgr.nc.Publish(mgr.respSubject(*ev), data); err != nil && mgr.logNatsError {
				_, _ = fmt.Fprintln(os.Stderr, "failed to publish to nats", err)
			}
		}, loggerOpts)
		ctx = logr.NewContext(ctx, logger)
	} else {
		ctx = logr.NewContext(ctx, logr.Discard())
	}

	data := def.NewObj()
	if err = ev.DataAs(data); err != nil {
		return errors.Wrap(err, "failed to unmarshal event data")
	}

	// report start
	title := getTitle(*ev)
	msgTitle := title + " started!"
	if mgr.sendUpdates(*ev) {
		mgr.mustPublish(mgr.respSubject(*ev), mgr.newResponse(TaskStatusStarted, ev.ID(), title, msgTitle, nil))
	}
	if mgr.sendNotification(*ev) {
		mgr.mustPublish(mgr.notificationSubj(*ev), mgr.newResponse(TaskStatusStarted, ev.ID(), title, msgTitle, nil))
	}

	// invoke fn
	{
		parms := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(data).Elem(),
		}
		results := reflect.ValueOf(def.Fn()).Call(parms)
		fnErr, _ := results[0].Interface().(error)
		// WARNING: https://stackoverflow.com/a/46275411/244009
		if fnErr != nil && !results[0].IsNil() /*for error wrapper interfaces*/ {
			err = fnErr
			return
		}
	}

	return nil
}

type TaskResponse struct {
	ID      string `json:"id,omitempty"`
	Subject string `json:"subject,omitempty"`
}

const (
	EventExtTitle  = "title"
	EventExtRespID = "respID"
	EventExtNotify = "notify"
)

func (mgr *TaskManager) Submit(t tasks.TaskType, tenantID, taskID, respID, title string, data any, notify bool) (*TaskResponse, error) {
	if taskID == "" {
		taskID = xid.New().String()
	}

	ev := cloudeventssdk.NewEvent()
	ev.SetID(taskID) // some id from request body

	// /byte.builders/auditor/license_id/feature/info.ProductName/api_group/api_resource/
	// ref: https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#source-1
	// ev.SetSource(fmt.Sprintf("/byte.builders/platform-apiserver/%s", hostname))
	ev.SetSource(mgr.name)
	// obj.getUID
	// ref: https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#subject

	// sub := fmt.Sprintf("/byte.builders/users/%d", 1)
	ev.SetSubject(tenantID)
	// builders.byte.background_tasks.{created, updated, deleted}.v1
	// ref: https://github.com/cloudevents/spec/blob/v1.0.1/spec.md#type

	// taskType := "builders.byte.background_tasks.install_chart.v1"
	ev.SetType(string(t))
	ev.SetTime(time.Now().UTC())

	ev.SetExtension(EventExtTitle, title)
	ev.SetExtension(EventExtRespID, respID)
	ev.SetExtension(EventExtNotify, strconv.FormatBool(notify))

	if err := ev.SetData(cloudeventssdk.ApplicationJSON, data); err != nil {
		return nil, errors.Wrapf(err, "failed to marshal data into json tenantID=%s msgID=%s taskType=%s", tenantID, taskID, t)
	}

	var msg nats.Msg
	var err error
	msg.Subject = mgr.taskSubject(ev)
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	msg.Header.Set(nats.MsgIdHdr, ev.ID())
	if msg.Data, err = ev.MarshalJSON(); err != nil {
		return nil, errors.Wrapf(err, "failed to marshal event into json tenantID=%s msgID=%s taskType=%s", tenantID, taskID, t)
	}
	if _, err = mgr.nc.RequestMsg(&msg, mgr.requestTimeout); err != nil {
		return nil, errors.Wrapf(err, "failed to submit task tenantID=%s msgID=%s taskType=%s", tenantID, taskID, t)
	}

	return &TaskResponse{
		ID:      taskID,
		Subject: mgr.respSubject(ev),
	}, nil
}

func (mgr *TaskManager) taskSubject(ev cloudeventssdk.Event) string {
	return fmt.Sprintf("%s.queue.%s", mgr.stream, ev.Subject())
}

func (mgr *TaskManager) sendUpdates(ev cloudeventssdk.Event) bool {
	if mgr.responseSubjectPrefix == "" {
		return false
	}
	var s string
	err := ev.ExtensionAs(EventExtRespID, &s)
	return err == nil && s != ""
}

func (mgr *TaskManager) respSubject(ev cloudeventssdk.Event) string {
	if mgr.sendUpdates(ev) {
		return fmt.Sprintf("%s.%s.%s", mgr.responseSubjectPrefix, ev.Subject(), getRespID(ev))
	}
	return ""
}

func (mgr *TaskManager) notificationSubj(ev cloudeventssdk.Event) string {
	return fmt.Sprintf("%s.%s", mgr.notificationSubjectPrefix, ev.Subject())
}

func (mgr *TaskManager) sendNotification(ev cloudeventssdk.Event) bool {
	if mgr.notificationSubjectPrefix == "" || ev.Subject() == "" {
		return false
	}
	notify, _ := types.ToBool(ev.Extensions()[EventExtNotify])
	return notify
}

func (mgr *TaskManager) mustPublish(subj string, data []byte) {
	if err := mgr.nc.Publish(subj, data); err != nil && mgr.logNatsError {
		klog.Errorln(err)
	}
}

type TaskStatus string

const (
	TaskStatusPending = "Pending"
	TaskStatusStarted = "Started"
	TaskStatusRunning = "Running"
	TaskStatusFailed  = "Failed"
	TaskStatusSuccess = "Success"
)

func (mgr *TaskManager) newResponse(status TaskStatus, id, step, msg string, err error) []byte {
	m := map[string]string{
		"status": string(status),
		"msg":    msg,
	}
	if id != "" {
		m["id"] = id
	}
	if step != "" {
		m["step"] = step
	}
	if err != nil {
		m["error"] = err.Error()
	}
	data, _ := json.Marshal(m)
	return data
}

func (mgr *TaskManager) logResponse(id, args string) []byte {
	return []byte(fmt.Sprintf(`{"id":%q,"status":%q,%s`, id, TaskStatusRunning, args[1:]))
}

func getTitle(ev cloudeventssdk.Event) string {
	var s string
	if e2 := ev.ExtensionAs(EventExtTitle, &s); e2 != nil {
		s = "Task " + ev.ID()
	}
	return s
}

func getRespID(ev cloudeventssdk.Event) string {
	var s string
	if err := ev.ExtensionAs(EventExtRespID, &s); err != nil {
		panic(errors.Wrap(err, "event missing "+EventExtRespID))
	}
	return s
}
