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
	AckWait        time.Duration

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
		RequestTimeout:            5 * time.Second,
		AckWait:                   1 * time.Hour,
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

type TaskManager struct {
	nc             *nats.Conn
	workerConsumer jetstream.Consumer
	requestTimeout time.Duration
	ackWait        time.Duration

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
	inflight map[uint64]jetstream.Msg
	// nextClaim is the source of monotonic claim tokens.
	nextClaim uint64
	// draining is set once the manager's context is cancelled, after which
	// workers stop claiming new messages.
	draining bool
}

func New(nc *nats.Conn, opts Options) *TaskManager {
	return &TaskManager{
		nc:                        nc,
		requestTimeout:            opts.RequestTimeout,
		ackWait:                   opts.AckWait,
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
		mgr.inflight = map[uint64]jetstream.Msg{}
	}
	mgr.nextClaim++
	mgr.inflight[mgr.nextClaim] = msg
	return mgr.nextClaim, true
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
func (mgr *TaskManager) drainInflight() {
	mgr.inflightMu.Lock()
	defer mgr.inflightMu.Unlock()
	mgr.draining = true
	for token, msg := range mgr.inflight {
		if err := msg.Nak(); err != nil && mgr.logNatsError {
			klog.ErrorS(err, "failed to NAK in-flight natjobs msg on shutdown", "id", msg.Headers().Get(nats.MsgIdHdr))
		}
		delete(mgr.inflight, token)
	}
}

func (mgr *TaskManager) Start(ctx context.Context, createConsumer bool) error {
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
		MaxDeliver:    5,
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
	}()

	klog.Info("Starting workers")
	// Launch workers to process and proxy the message to relevant subject from nats subject
	for i := 0; i < mgr.numWorkersPerReplica; i++ {
		go wait.Until(func() { mgr.runWorker(ctx) }, 5*time.Second, ctx.Done())
	}

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

	var ev *cloudeventssdk.Event

	defer func() {
		if ev != nil {
			var msg string
			var status TaskStatus
			if err != nil {
				msg = getTitle(*ev) + " failed!"
				status = TaskStatusFailed
			} else {
				msg = getTitle(*ev) + " completed successfully!"
				status = TaskStatusSuccess
			}
			if mgr.sendUpdates(*ev) {
				mgr.mustPublish(mgr.respSubject(*ev), mgr.newResponse(status, ev.ID(), "", msg, err))
			}
			if mgr.sendNotification(*ev) {
				mgr.mustPublish(mgr.notificationSubj(*ev), mgr.newResponse(status, ev.ID(), "", msg, err))
			}
		}

		// Only ack if we still own the message. If shutdown NAK'd it while the
		// task was running, it has already been redelivered, so acking here
		// would be a no-op at best and could ack a message another worker is now
		// processing.
		if !mgr.releaseMsg(token) {
			return
		}
		// report failure ?
		if e2 := msg.Ack(); e2 != nil && mgr.logNatsError {
			klog.ErrorS(err, "failed ACK msg", "id", msg.Headers().Get(nats.MsgIdHdr))
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

	ctx := context.Background()
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
