package integrationtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nirantarasi/wasq"
)

func TestPostgresClientEnqueue_TableDriven(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	client, err := wasq.NewClientFromDB(db)
	if err != nil {
		t.Fatalf("failed to create postgres client: %v", err)
	}
	defer client.Close()

	tests := []struct {
		name      string
		taskType  string
		payload   []byte
		opts      []wasq.Option
		headers   map[string]string
		wantQueue string
		wantRetry int
	}{
		{
			name:      "immediate task with default options",
			taskType:  "pg:simple",
			payload:   []byte("postgres-payload-1"),
			wantQueue: "default",
			wantRetry: 25,
		},
		{
			name:      "task with custom queue",
			taskType:  "pg:custom_queue",
			payload:   []byte("custom-queue-payload"),
			opts:      []wasq.Option{wasq.Queue("pg_special")},
			wantQueue: "pg_special",
			wantRetry: 25,
		},
		{
			name:      "task with custom max retry",
			taskType:  "pg:retry_task",
			payload:   []byte("retry-payload"),
			opts:      []wasq.Option{wasq.MaxRetry(5)},
			wantQueue: "default",
			wantRetry: 5,
		},
		{
			name:      "task with custom headers",
			taskType:  "pg:headers_task",
			payload:   []byte("headers-payload"),
			headers:   map[string]string{"tenant": "acme", "user": "123"},
			wantQueue: "default",
			wantRetry: 25,
		},
		{
			name:      "scheduled task with ProcessIn",
			taskType:  "pg:scheduled_task",
			payload:   []byte("scheduled-payload"),
			opts:      []wasq.Option{wasq.ProcessIn(10 * time.Minute)},
			wantQueue: "default",
			wantRetry: 25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var task *wasq.Task
			if len(tt.headers) > 0 {
				task = wasq.NewTaskWithHeaders(tt.taskType, tt.payload, tt.headers, tt.opts...)
			} else {
				task = wasq.NewTask(tt.taskType, tt.payload, tt.opts...)
			}

			info, err := client.Enqueue(task)
			if err != nil {
				t.Fatalf("client.Enqueue failed: %v", err)
			}

			if info.Type != tt.taskType {
				t.Errorf("info.Type = %q, want %q", info.Type, tt.taskType)
			}
			if string(info.Payload) != string(tt.payload) {
				t.Errorf("info.Payload = %s, want %s", info.Payload, tt.payload)
			}
			if info.Queue != tt.wantQueue {
				t.Errorf("info.Queue = %q, want %q", info.Queue, tt.wantQueue)
			}
			if info.MaxRetry != tt.wantRetry {
				t.Errorf("info.MaxRetry = %d, want %d", info.MaxRetry, tt.wantRetry)
			}
		})
	}
}

func TestPostgresServerProcessTasks_TableDriven(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	client, err := wasq.NewClientFromDB(db)
	if err != nil {
		t.Fatalf("failed to create postgres client: %v", err)
	}
	defer client.Close()

	tests := []struct {
		name      string
		taskType  string
		payload   []byte
		headers   map[string]string
		taskOpts  []wasq.Option
		serverCfg wasq.Config
		handler   func(done chan struct{}, executionCount *int32) wasq.HandlerFunc
		timeout   time.Duration
	}{
		{
			name:      "postgres worker processes immediate task",
			taskType:  "pg:exec_simple",
			payload:   []byte("pg-payload-ok"),
			serverCfg: wasq.Config{Concurrency: 2},
			handler: func(done chan struct{}, executionCount *int32) wasq.HandlerFunc {
				return func(ctx context.Context, task *wasq.Task) error {
					atomic.AddInt32(executionCount, 1)
					if string(task.Payload()) != "pg-payload-ok" {
						t.Errorf("got payload %s, want %s", task.Payload(), "pg-payload-ok")
					}
					close(done)
					return nil
				}
			},
			timeout: 5 * time.Second,
		},
		{
			name:      "postgres worker processes task with headers",
			taskType:  "pg:exec_headers",
			payload:   []byte("pg-headers-data"),
			headers:   map[string]string{"env": "staging"},
			serverCfg: wasq.Config{Concurrency: 2},
			handler: func(done chan struct{}, executionCount *int32) wasq.HandlerFunc {
				return func(ctx context.Context, task *wasq.Task) error {
					atomic.AddInt32(executionCount, 1)
					if task.Headers()["env"] != "staging" {
						t.Errorf("header env = %q, want %q", task.Headers()["env"], "staging")
					}
					close(done)
					return nil
				}
			},
			timeout: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan struct{})
			var count int32

			server, err := wasq.NewServerFromDB(db, tt.serverCfg)
			if err != nil {
				t.Fatalf("NewServerFromDB failed: %v", err)
			}

			handler := tt.handler(done, &count)
			if err := server.Start(handler); err != nil {
				t.Fatalf("server.Start failed: %v", err)
			}
			defer server.Shutdown()

			var task *wasq.Task
			if len(tt.headers) > 0 {
				task = wasq.NewTaskWithHeaders(tt.taskType, tt.payload, tt.headers, tt.taskOpts...)
			} else {
				task = wasq.NewTask(tt.taskType, tt.payload, tt.taskOpts...)
			}

			if _, err := client.Enqueue(task); err != nil {
				t.Fatalf("client.Enqueue failed: %v", err)
			}

			select {
			case <-done:
				// success
			case <-time.After(tt.timeout):
				t.Fatalf("timed out waiting for task processing in postgres after %v", tt.timeout)
			}

			server.Shutdown()

			if atomic.LoadInt32(&count) != 1 {
				t.Errorf("expected 1 execution, got %d", count)
			}
		})
	}
}

func TestPostgresMultipleQueues_TableDriven(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	client, err := wasq.NewClientFromDB(db)
	if err != nil {
		t.Fatalf("failed to create postgres client: %v", err)
	}
	defer client.Close()

	var count atomic.Int32
	var wg sync.WaitGroup
	wg.Add(3)

	handler := wasq.HandlerFunc(func(ctx context.Context, task *wasq.Task) error {
		count.Add(1)
		wg.Done()
		return nil
	})

	server, err := wasq.NewServerFromDB(db, wasq.Config{
		Concurrency: 4,
		Queues: map[string]int{
			"q_high": 3,
			"q_med":  2,
			"q_low":  1,
		},
	})
	if err != nil {
		t.Fatalf("NewServerFromDB failed: %v", err)
	}

	if err := server.Start(handler); err != nil {
		t.Fatalf("server.Start failed: %v", err)
	}
	defer server.Shutdown()

	tasks := []struct {
		queue   string
		payload string
	}{
		{"q_high", "high-task"},
		{"q_med", "med-task"},
		{"q_low", "low-task"},
	}

	for _, tsk := range tasks {
		_, err := client.Enqueue(wasq.NewTask("multi:q", []byte(tsk.payload), wasq.Queue(tsk.queue)))
		if err != nil {
			t.Fatalf("failed to enqueue to %q: %v", tsk.queue, err)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for tasks from multiple queues")
	}

	server.Shutdown()

	if count.Load() != 3 {
		t.Errorf("expected 3 tasks executed, got %d", count.Load())
	}
}
