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
	"strings"
	"time"

	"gomodules.xyz/natjobs/tasks"

	cloudeventssdk "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/nats-io/nats.go"
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
	sub            *nats.Subscription
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
}

func New(nc *nats.Conn, opts Options) *TaskManager {
	return &TaskManager{
		nc: nc,
		//	sub:                       nil,
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

func (mgr *TaskManager) Start(ctx context.Context, jsmOpts ...nats.JSOpt) error {
	// create stream
	jsm, err := mgr.nc.JetStream(jsmOpts...)
	if err != nil {
		return err
	}

	streamInfo, err := jsm.StreamInfo(mgr.stream, jsmOpts...)

	if streamInfo == nil || err != nil && err.Error() == "nats: stream not found" {
		_, err = jsm.AddStream(&nats.StreamConfig{
			Name:     mgr.stream,
			Subjects: []string{mgr.stream + ".queue.*"},
			// https://docs.nats.io/nats-concepts/core-nats/queue#stream-as-a-queue
			Retention:  nats.WorkQueuePolicy,
			MaxMsgs:    -1,
			MaxBytes:   -1,
			Discard:    nats.DiscardOld,
			MaxAge:     30 * 24 * time.Hour, // 30 days
			MaxMsgSize: 1 * 1024 * 1024,     // 1 MB
			Storage:    nats.FileStorage,
			Replicas:   1, // TODO: configure
			Duplicates: time.Hour,
		})
		if err != nil {
			return err
		}
	}

	// create nats consumer
	consumerName := "workers"
	ackPolicy := nats.AckExplicitPolicy
	_, err = jsm.AddConsumer(mgr.stream, &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: ackPolicy,
		AckWait:   mgr.ackWait, // TODO: max for any task type
		// The number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored
		MaxWaiting: 1,
		// max working set
		MaxAckPending: mgr.numReplicas * mgr.numWorkersPerReplica,
		// one request per worker
		MaxRequestBatch: 1,
		// max_expires the max amount of time that a pull request with an expires should be allowed to remain active
		MaxRequestExpires: 1 * time.Second,
		DeliverPolicy:     nats.DeliverAllPolicy,
		MaxDeliver:        5,
		FilterSubject:     "",
		ReplayPolicy:      nats.ReplayInstantPolicy,
	})
	if err != nil {
		return err
	}
	sub, err := jsm.PullSubscribe("", consumerName, nats.Bind(mgr.stream, consumerName))
	if err != nil {
		return err
	}
	mgr.sub = sub

	// start workers
	klog.Info("Starting workers")
	// Launch two workers to process Foo resources
	for i := 0; i < mgr.numWorkersPerReplica; i++ {
		go wait.Until(mgr.runWorker, 5*time.Second, ctx.Done())
	}

	return nil
}

func (mgr *TaskManager) runWorker() {
	for {
		err := mgr.processNextMsg()
		if err != nil {
			if mgr.logNatsError && !strings.Contains(err.Error(), nats.ErrTimeout.Error()) {
				klog.Errorln(err)
			}
			break
		}
	}
}

func (mgr *TaskManager) processNextMsg() (err error) {
	var msgs []*nats.Msg
	msgs, err = mgr.sub.Fetch(1, nats.MaxWait(50*time.Millisecond))
	if err != nil || len(msgs) == 0 {
		// no more msg to process
		err = errors.Wrap(err, "failed to fetch msg")
		return
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
				mgr.mustPublish(mgr.respSubject(*ev), mgr.newResponse(status, "", msg, err))
			}
			if mgr.sendNotification(*ev) {
				mgr.mustPublish(mgr.notificationSubj(*ev), mgr.newResponse(status, "", msg, err))
			}
		}

		// report failure ?
		if e2 := msgs[0].Ack(); e2 != nil && mgr.logNatsError {
			klog.ErrorS(err, "failed ACK msg", "id", msgs[0].Header.Get(nats.MsgIdHdr))
		}
	}()

	newEvent := cloudeventssdk.NewEvent()
	err = newEvent.UnmarshalJSON(msgs[0].Data)
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
			data := mgr.logResponse(obj)
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
	msg := title + " started!"
	if mgr.sendUpdates(*ev) {
		mgr.mustPublish(mgr.respSubject(*ev), mgr.newResponse(TaskStatusStarted, title, msg, nil))
	}
	if mgr.sendNotification(*ev) {
		mgr.mustPublish(mgr.notificationSubj(*ev), mgr.newResponse(TaskStatusStarted, title, msg, nil))
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
)

func (mgr *TaskManager) Submit(t tasks.TaskType, tenantID, taskID, respID, title string, data any) (*TaskResponse, error) {
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
	return mgr.notificationSubjectPrefix != "" && ev.Subject() != ""
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

func (mgr *TaskManager) newResponse(status TaskStatus, step, msg string, err error) []byte {
	m := map[string]string{
		"status": string(status),
		"msg":    msg,
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

func (mgr *TaskManager) logResponse(args string) []byte {
	return []byte(fmt.Sprintf(`{"status":%q,%s`, TaskStatusRunning, args[1:]))
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
