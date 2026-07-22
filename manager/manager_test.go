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
	// Keep AckWait at its (large) default so the redelivery test proves the NAK
	// path rather than accidentally passing because AckWait elapsed.
	return opts
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
	// task up again well within AckWait (1h) — proving redelivery came from the
	// NAK and not from AckWait expiring.
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
