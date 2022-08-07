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

type TaskManager struct {
	nc             *nats.Conn
	sub            *nats.Subscription
	requestTimeout time.Duration

	// same as stream
	stream string

	// manager id, < 0 means auto detect
	// id int
	// hostname
	name string

	numReplicas int
	numWorkers  int

	// sends
	notificationSubjectPrefix string

	// TODO: send org level notification
	// sendOrgNotification bool

	logErrors bool
}

func New() *TaskManager {
	return new(TaskManager)
}

func (mgr *TaskManager) Start(ctx context.Context, jsmOpts ...nats.JSOpt) error {
	// create stream
	jsm, err := mgr.nc.JetStream(jsmOpts...)
	if err != nil {
		return err
	}

	streamInfo, err := jsm.StreamInfo(mgr.stream, jsmOpts...)

	if streamInfo == nil || err != nil && err.Error() == "stream not found" {
		_, err = jsm.AddStream(&nats.StreamConfig{
			Name:     mgr.stream,
			Subjects: []string{mgr.stream + ".queue.*"},
		})
		// TODO: return error other than stream already exists error
		if err != nil {
			return err
		}
	}

	// create nats consumer
	consumerName := "workers"
	ackWait := 10 * time.Second // TODO: per task basis?
	ackPolicy := nats.AckExplicitPolicy
	_, err = jsm.AddConsumer(mgr.stream, &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: ackPolicy,
		AckWait:   ackWait, // TODO: configure ?
		// The number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored
		MaxWaiting: 1,
		// max working set
		MaxAckPending: mgr.numWorkers * mgr.numReplicas,
		// one request per worker
		MaxRequestBatch: 1,
		// max_expires the max amount of time that a pull request with an expires should be allowed to remain active
		MaxRequestExpires: 1 * time.Second,
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
	for i := 0; i < mgr.numWorkers; i++ {
		go wait.Until(mgr.runWorker, time.Second, ctx.Done())
	}

	return nil
}

func (mgr *TaskManager) runWorker() {
	for {
		err := mgr.processNextMsg()
		if err != nil {
			if mgr.logErrors {
				klog.Errorln(err)
			}
			break
		}
	}
}

func (mgr *TaskManager) processNextMsg() (err error) {
	var msgs []*nats.Msg
	msgs, err = mgr.sub.Fetch(1, nats.MaxWait(100*time.Millisecond))
	if err != nil {
		// no more msg to process
		err = errors.Wrap(err, "failed to fetch msg")
		return
	}

	ev := cloudeventssdk.NewEvent()
	var rawReq RawTaskRequest
	eventDecoded := false

	defer func() {
		if eventDecoded {
			if rawReq.Description == "" {
				rawReq.Description = "Task " + ev.ID()
			}

			var status string
			var seqId int64
			if err != nil {
				status = rawReq.Description + " failed!"
				seqId = 1
			} else {
				status = rawReq.Description + " completed successfully!"
				seqId = 0
			}
			mgr.mustPublish(mgr.respSubject(ev), mgr.newResponse(seqId, status))
			if mgr.sendNotification(ev) {
				mgr.mustPublish(mgr.notificationSubj(ev), []byte(status))
			}
		}

		// report failure ?
		if e2 := msgs[0].Ack(); e2 != nil && mgr.logErrors {
			klog.ErrorS(err, "failed ACK msg", "id", msgs[0].Header.Get(nats.MsgIdHdr))
		}
	}()

	err = ev.UnmarshalJSON(msgs[0].Data)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal event")
	}
	eventDecoded = true

	def, ok := tasks.Get(tasks.TaskType(ev.Type()))
	if !ok {
		return errors.Errorf("No TaskDef registered for task type %s", ev.Type())
	}

	ctx := context.Background()

	loggerOpts := funcr.Options{}
	if def.RespLoggerOpts() != nil {
		loggerOpts = *def.RespLoggerOpts()
	}
	logger := funcr.New(func(_, args string) {
		data := mgr.newResponse(time.Now().UnixMicro(), args)
		if err := mgr.nc.Publish(mgr.respSubject(ev), data); err != nil && mgr.logErrors {
			_, _ = fmt.Fprintln(os.Stderr, "failed to publish to nats", err)
		}
	}, loggerOpts)
	ctx = logr.NewContext(ctx, logger)

	if err = ev.DataAs(&rawReq); err != nil {
		return errors.Wrap(err, "failed to unmarshal event data")
	}
	data := def.NewObj()
	if err = json.Unmarshal(rawReq.Data, data); err != nil {
		return errors.Wrap(err, "failed to unmarshal request data")
	}

	// report start
	status := rawReq.Description + " started"
	mgr.mustPublish(mgr.respSubject(ev), mgr.newResponse(-1, status))
	if mgr.sendNotification(ev) {
		mgr.mustPublish(mgr.notificationSubj(ev), []byte(status))
	}

	// invoke fn
	{
		parms := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(data),
		}
		results := reflect.ValueOf(def.Fn()).Call(parms)
		fnErr, _ := results[0].Interface().(error)
		// WARNING: https://stackoverflow.com/a/46275411/244009
		if fnErr != nil && !reflect.ValueOf(fnErr).IsNil() /*for error wrapper interfaces*/ {
			err = fnErr
			return
		}
	}

	return nil
}

type TaskRequest struct {
	Description string `json:"d"`
	Data        any    `json:"data"`
}

type RawTaskRequest struct {
	Description string          `json:"d"`
	Data        json.RawMessage `json:"data"`
}

type TaskResponse struct {
	Subject string
}

func (mgr *TaskManager) Submit(tenantID string, t tasks.TaskType, msgID, desc string, data any) (*TaskResponse, error) {
	if msgID == "" {
		msgID = xid.New().String()
	}

	ev := cloudeventssdk.NewEvent()
	ev.SetID(msgID) // some id from request body

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

	if err := ev.SetData(cloudeventssdk.ApplicationJSON, TaskRequest{
		Description: desc,
		Data:        data,
	}); err != nil {
		return nil, errors.Wrapf(err, "failed to marshal data into json tenantID=%s msgID=%s taskType=%s", tenantID, msgID, t)
	}

	var msg nats.Msg
	var err error
	msg.Subject = mgr.taskSubject(ev)
	msg.Header.Set(nats.MsgIdHdr, ev.ID())
	if msg.Data, err = ev.MarshalJSON(); err != nil {
		return nil, errors.Wrapf(err, "failed to marshal event into json tenantID=%s msgID=%s taskType=%s", tenantID, msgID, t)
	}
	if _, err = mgr.nc.RequestMsg(&msg, mgr.requestTimeout); err != nil {
		return nil, errors.Wrapf(err, "failed to submit task tenantID=%s msgID=%s taskType=%s", tenantID, msgID, t)
	}

	return &TaskResponse{
		Subject: mgr.respSubject(ev),
	}, nil
}

func (mgr *TaskManager) taskSubject(ev cloudeventssdk.Event) string {
	return fmt.Sprintf("%s.queue.%s", mgr.stream, ev.Subject())
}

func (mgr *TaskManager) respSubject(ev cloudeventssdk.Event) string {
	return fmt.Sprintf("%s.resp.%s.%s.%s", mgr.stream, ev.Subject(), ev.Source(), ev.ID())
}

func (mgr *TaskManager) notificationSubj(ev cloudeventssdk.Event) string {
	return fmt.Sprintf("%s.%s", mgr.notificationSubjectPrefix, ev.Subject())
}

func (mgr *TaskManager) sendNotification(ev cloudeventssdk.Event) bool {
	return mgr.notificationSubjectPrefix != "" && ev.Subject() != ""
}

func (mgr *TaskManager) mustPublish(subj string, data []byte) {
	if err := mgr.nc.Publish(subj, data); err != nil && mgr.logErrors {
		klog.Errorln(err)
	}
}

func (mgr *TaskManager) newResponse(seqId int64, args string) []byte {
	if strings.HasPrefix(args, "{") {
		return []byte(fmt.Sprintf(`{"seqId": %d, "msg": %s}`, seqId, args))
	}

	data, err := json.Marshal(map[string]any{
		"seqId": seqId,
		"msg":   args,
	})
	if err != nil && mgr.logErrors {
		klog.ErrorS(err, "failed to prepare ")
	}
	return data
}
