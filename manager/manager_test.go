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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gomodules.xyz/natjobs/tasks"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type testPayload struct {
	Foo string `json:"foo"`
}

// runJetStreamServer starts an in-process NATS server with JetStream enabled on
// a random port (see setting.SetTestNatsServerConfiguration for the pattern)
// and returns a connection to it. The server and connection are torn down when
// the test finishes.
func runJetStreamServer(t *testing.T) *nats.Conn {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1 // pick a free port so tests don't collide
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	s := natsserver.RunServer(&opts)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatalf("failed to connect to test nats server: %v", err)
	}

	t.Cleanup(func() {
		nc.Close()
		s.Shutdown()
		s.WaitForShutdown()
	})
	return nc
}

func testOptions() Options {
	opts := DefaultOptions()
	opts.NumWorkers = 1
	return opts
}

// newTestConn opens a second connection to the same server as nc, so a test can
// tear one connection down without disturbing the other -- which is how a
// replaced pod is simulated.
func newTestConn(t *testing.T, nc *nats.Conn) *nats.Conn {
	t.Helper()

	c, err := nats.Connect(nc.ConnectedUrl())
	if err != nil {
		t.Fatalf("failed to open a second connection to the test nats server: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSubmitOnlyModeDoesNotCreateWorkerConsumer verifies that Start(ctx, false)
// ensures the stream exists (so tasks can be submitted) but does not create the
// durable "workers" consumer or launch workers. This is what the short-lived
// setup job relies on to avoid fetching a task and then exiting before ack.
func TestSubmitOnlyModeDoesNotCreateWorkerConsumer(t *testing.T) {
	nc := runJetStreamServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := New(nc, testOptions())
	if err := mgr.Start(ctx, false); err != nil {
		t.Fatalf("Start(ctx, false): %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream(): %v", err)
	}

	// The stream must exist so a submit-only caller can publish tasks.
	if _, err := js.StreamInfo(mgr.stream); err != nil {
		t.Fatalf("expected stream %q to exist in submit-only mode: %v", mgr.stream, err)
	}

	// The worker consumer must NOT be created in submit-only mode.
	if _, err := js.ConsumerInfo(mgr.stream, "workers"); !errors.Is(err, nats.ErrConsumerNotFound) {
		t.Fatalf("expected no %q consumer in submit-only mode, got err=%v", "workers", err)
	}
}

// TestConsumerModeProcessesSubmittedTask is a happy-path check: Start(ctx, true)
// creates the consumer, launches workers, and a submitted task is processed.
func TestConsumerModeProcessesSubmittedTask(t *testing.T) {
	nc := runJetStreamServer(t)

	const taskType = tasks.TaskType("test.process.v1")
	done := make(chan testPayload, 1)
	tasks.MustRegister(taskType, func(_ context.Context, in testPayload) error {
		done <- in
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := New(nc, testOptions())
	if err := mgr.Start(ctx, true); err != nil {
		t.Fatalf("Start(ctx, true): %v", err)
	}

	if _, err := mgr.Submit(taskType, "tenant1", "", "", "process", testPayload{Foo: "hello"}, false); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case got := <-done:
		if got.Foo != "hello" {
			t.Fatalf("unexpected payload: %+v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("task was not processed within timeout")
	}
}

// TestGracefulShutdownNaksInflightForImmediateRedelivery verifies that when a
// manager's context is cancelled while a task is in flight, the task is NAK'd
// (redelivered immediately) instead of stalling for a full AckWait, and that
// the shared durable consumer is left intact so other replicas keep working.
func TestGracefulShutdownNaksInflightForImmediateRedelivery(t *testing.T) {
	nc := runJetStreamServer(t)

	const taskType = tasks.TaskType("test.redelivery.v1")
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	var runs int32
	tasks.MustRegister(taskType, func(_ context.Context, _ testPayload) error {
		atomic.AddInt32(&runs, 1)
		started <- struct{}{}
		<-release // hold the task in flight until the test releases it
		return nil
	})

	// Manager A starts processing the task and is then shut down mid-flight.
	ctxA, cancelA := context.WithCancel(context.Background())
	mgrA := New(nc, testOptions())
	if err := mgrA.Start(ctxA, true); err != nil {
		t.Fatalf("mgrA.Start: %v", err)
	}

	if _, err := mgrA.Submit(taskType, "tenant1", "", "", "redelivery", testPayload{Foo: "bar"}, false); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Wait until manager A has the task in flight.
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("task never started on manager A")
	}

	// Simulate SIGTERM on manager A. Its in-flight message must be NAK'd for
	// immediate redelivery, without deleting the shared consumer.
	cancelA()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream(): %v", err)
	}

	// The shared durable consumer must survive manager A's shutdown; otherwise a
	// partial (rolling) restart would break the replicas that were not restarted.
	if !waitFor(3*time.Second, func() bool {
		_, e := js.ConsumerInfo(mgrA.stream, "workers")
		return e == nil
	}) {
		t.Fatal("worker consumer should still exist after a graceful shutdown")
	}

	// Manager B represents a surviving / replacement replica. It must pick the
	// task up again far sooner than AckWait, proving redelivery came from the NAK
	// and not from AckWait expiring.
	ctxB, cancelB := context.WithCancel(context.Background())
	mgrB := New(nc, testOptions())
	if err := mgrB.Start(ctxB, true); err != nil {
		t.Fatalf("mgrB.Start: %v", err)
	}

	select {
	case <-started:
		// redelivered and started again
	case <-time.After(10 * time.Second):
		t.Fatal("task was not redelivered after graceful shutdown (would have waited a full AckWait)")
	}

	// Let both invocations return so their defers run.
	close(release)

	if got := atomic.LoadInt32(&runs); got < 2 {
		t.Fatalf("expected the task to run at least twice (original + redelivery), got %d", got)
	}

	// Manager B must eventually ack the redelivered task, removing it from the
	// work queue (so it is not stuck being redelivered forever). Waiting for
	// this also lets B finish its ack on the live connection before teardown.
	if !waitFor(10*time.Second, func() bool {
		si, e := js.StreamInfo(mgrB.stream)
		return e == nil && si.State.Msgs == 0
	}) {
		t.Fatal("redelivered task was never acked / drained from the work queue")
	}

	// Stop manager B while the connection is still up, so its shutdown drain
	// (which has nothing in flight now) runs cleanly.
	cancelB()
}

// TestDrainedGatesConnectionTeardown exercises the shutdown sequence a host
// process must run: cancel, wait for Drained, and only then tear the NATS
// connection down. A NAK is a publish, so draining the connection first rejects
// it and strands the task for a full AckWait with nothing but a log line to show
// for it.
func TestDrainedGatesConnectionTeardown(t *testing.T) {
	nc := runJetStreamServer(t)
	ncA := newTestConn(t, nc)

	const taskType = tasks.TaskType("test.drained.v1")
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	tasks.MustRegister(taskType, func(_ context.Context, _ testPayload) error {
		started <- struct{}{}
		<-release // hold the task in flight until the test releases it
		return nil
	})

	opts := testOptions()
	// An hour-long AckWait and no heartbeat, so only a NAK can explain a fast
	// redelivery below.
	opts.AckWait = time.Hour
	opts.Heartbeat = 0

	ctxA, cancelA := context.WithCancel(context.Background())
	mgrA := New(ncA, opts)
	if err := mgrA.Start(ctxA, true); err != nil {
		t.Fatalf("mgrA.Start: %v", err)
	}

	if _, err := mgrA.Submit(taskType, "tenant1", "", "", "drained", testPayload{Foo: "bar"}, false); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("task never started on manager A")
	}

	// The shutdown sequence a host process must run. Closing the connection here
	// instead of draining it is deliberate: Drain returns before it has stopped
	// accepting publishes, so it only loses the NAK some of the time, whereas
	// Close reproduces the lost NAK every run. Moving the Close above the wait
	// fails this test.
	cancelA()
	select {
	case <-mgrA.Drained():
	case <-time.After(10 * time.Second):
		t.Fatal("manager A never reported its in-flight messages released")
	}
	ncA.Close()

	// The replacement replica must get the task back immediately.
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	mgrB := New(nc, opts)
	if err := mgrB.Start(ctxB, true); err != nil {
		t.Fatalf("mgrB.Start: %v", err)
	}

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("task was not redelivered after an ordered shutdown (the NAK never reached the server)")
	}
	close(release)
}

// TestDrainedClosedWhenWorkersNeverStarted makes sure Drained is safe to wait on
// even when this manager never launched workers, e.g. a submit-only setup job.
// Otherwise a host process' shutdown sequence would block on it forever.
func TestDrainedClosedWhenWorkersNeverStarted(t *testing.T) {
	nc := runJetStreamServer(t)

	mgr := New(nc, testOptions())
	if err := mgr.Start(context.Background(), false); err != nil {
		t.Fatalf("Start(ctx, false): %v", err)
	}

	select {
	case <-mgr.Drained():
	case <-time.After(3 * time.Second):
		t.Fatal("Drained must already be closed when no workers were started")
	}
}

// TestHeartbeatKeepsLongTaskFromRedelivery verifies the heartbeat does what a
// long AckWait used to: a task that runs for several times AckWait is not
// redelivered, so it is never executed twice. This is what lets AckWait shrink
// from "how long may a task take" to "how long may a worker be silent".
func TestHeartbeatKeepsLongTaskFromRedelivery(t *testing.T) {
	nc := runJetStreamServer(t)

	const taskType = tasks.TaskType("test.heartbeat.v1")
	var runs int32
	done := make(chan struct{}, 8)
	tasks.MustRegister(taskType, func(_ context.Context, _ testPayload) error {
		atomic.AddInt32(&runs, 1)
		time.Sleep(3 * time.Second) // several times AckWait
		done <- struct{}{}
		return nil
	})

	opts := testOptions()
	opts.AckWait = 1 * time.Second
	opts.Heartbeat = 200 * time.Millisecond
	opts.TaskTimeout = 1 * time.Minute
	// A second worker so a redelivery would actually be picked up (and detected)
	// rather than sitting undelivered behind the busy one.
	opts.NumWorkers = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := New(nc, opts)
	if err := mgr.Start(ctx, true); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := mgr.Submit(taskType, "tenant1", "", "", "heartbeat", testPayload{Foo: "bar"}, false); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("task never completed")
	}

	// Give a redelivery time to show up before declaring there was none.
	time.Sleep(2 * time.Second)
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("task ran %d times; heartbeats should have kept the server from redelivering it", got)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream(): %v", err)
	}
	if !waitFor(10*time.Second, func() bool {
		si, e := js.StreamInfo(mgr.stream)
		return e == nil && si.State.Msgs == 0
	}) {
		t.Fatal("heartbeated task was never acked / drained from the work queue")
	}
}

// TestUngracefulDeathRecoversAfterAckWait covers the case an ordered shutdown
// cannot: the process dies without running any shutdown path (SIGKILL after the
// grace period, OOM kill, node loss), so nothing NAKs. Recovery then costs
// exactly one AckWait, which is only tolerable because heartbeats let AckWait be
// short.
func TestUngracefulDeathRecoversAfterAckWait(t *testing.T) {
	nc := runJetStreamServer(t)
	ncA := newTestConn(t, nc)

	const taskType = tasks.TaskType("test.ungraceful.v1")
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	tasks.MustRegister(taskType, func(_ context.Context, _ testPayload) error {
		started <- struct{}{}
		<-release
		return nil
	})

	opts := testOptions()
	opts.AckWait = 2 * time.Second
	opts.Heartbeat = 500 * time.Millisecond
	opts.TaskTimeout = 1 * time.Minute

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	mgrA := New(ncA, opts)
	if err := mgrA.Start(ctxA, true); err != nil {
		t.Fatalf("mgrA.Start: %v", err)
	}
	if _, err := mgrA.Submit(taskType, "tenant1", "", "", "ungraceful", testPayload{Foo: "bar"}, false); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("task never started on manager A")
	}

	// Kill the connection without cancelling the context: no NAK is sent, and the
	// heartbeat dies with the connection, so the ack timer runs out.
	ncA.Close()

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	mgrB := New(nc, opts)
	if err := mgrB.Start(ctxB, true); err != nil {
		t.Fatalf("mgrB.Start: %v", err)
	}

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("task was never redelivered after an ungraceful death")
	}
	close(release)
}

// TestTaskTimeoutStopsHeartbeat pairs with the heartbeat: a task that ignores
// its context and wedges must stop being heartbeated once its deadline passes,
// so the server takes the task back instead of renewing it forever. Without this
// the heartbeat would trade a bounded stall for an unbounded one.
func TestTaskTimeoutStopsHeartbeat(t *testing.T) {
	nc := runJetStreamServer(t)

	const taskType = tasks.TaskType("test.wedged.v1")
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	tasks.MustRegister(taskType, func(_ context.Context, _ testPayload) error {
		started <- struct{}{}
		<-release // wedged: ignores ctx entirely, like a hung external call
		return nil
	})

	opts := testOptions()
	opts.AckWait = 2 * time.Second
	opts.Heartbeat = 300 * time.Millisecond
	opts.TaskTimeout = 500 * time.Millisecond
	// A second worker so the redelivery has somewhere to go while the first one
	// is still wedged.
	opts.NumWorkers = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := New(nc, opts)
	if err := mgr.Start(ctx, true); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := mgr.Submit(taskType, "tenant1", "", "", "wedged", testPayload{Foo: "bar"}, false); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("task never started")
	}

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("wedged task was never taken back; its heartbeat is still renewing the ack timer")
	}
	close(release)
}

// TestHandedBackTaskReportsNoTerminalStatus covers what a waiter sees when a
// worker is shut down mid-task. Shutdown cancels the task, so a well-behaved
// task function returns a context error -- but the task itself has only moved to
// another worker. Publishing Failed here would make the waiter (the ace-setup
// job, for one) treat a rescheduled task as a hard failure.
func TestHandedBackTaskReportsNoTerminalStatus(t *testing.T) {
	nc := runJetStreamServer(t)
	ncA := newTestConn(t, nc)

	const taskType = tasks.TaskType("test.handback.v1")
	started := make(chan struct{}, 8)
	tasks.MustRegister(taskType, func(ctx context.Context, _ testPayload) error {
		started <- struct{}{}
		<-ctx.Done() // a task that honours cancellation
		return ctx.Err()
	})

	opts := testOptions()
	opts.AckWait = time.Hour
	opts.Heartbeat = 0

	ctxA, cancelA := context.WithCancel(context.Background())
	mgrA := New(ncA, opts)
	if err := mgrA.Start(ctxA, true); err != nil {
		t.Fatalf("mgrA.Start: %v", err)
	}

	// respID has to be set for the manager to publish status updates at all.
	resp, err := mgrA.Submit(taskType, "tenant1", "", "resp1", "handback", testPayload{Foo: "bar"}, false)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	sub, err := nc.SubscribeSync(resp.Subject)
	if err != nil {
		t.Fatalf("SubscribeSync(%s): %v", resp.Subject, err)
	}
	defer sub.Unsubscribe() // nolint:errcheck

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("task never started on manager A")
	}

	cancelA()
	select {
	case <-mgrA.Drained():
	case <-time.After(10 * time.Second):
		t.Fatal("manager A never reported its in-flight messages released")
	}

	// Everything published on the response subject up to now must be non-terminal:
	// the task was reassigned, not finished.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, err := sub.NextMsg(500 * time.Millisecond)
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			t.Fatalf("NextMsg: %v", err)
		}
		var got struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(m.Data, &got); err != nil {
			t.Fatalf("failed to decode response %q: %v", m.Data, err)
		}
		if got.Status == TaskStatusFailed || got.Status == TaskStatusSuccess {
			t.Fatalf("a handed-back task reported terminal status %q; waiters would give up on a task that is being retried", got.Status)
		}
	}
}
