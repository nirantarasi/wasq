package integrationtest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nirantarasi/wasq"
)

func TestServerProcessTasks_TableDriven(t *testing.T) {
	redisOpt, cleanup := setupRedisContainer(t)
	defer cleanup()

	client := wasq.NewClient(redisOpt)
	defer client.Close()

	tests := []struct {
		name       string
		taskType   string
		payload    []byte
		headers    map[string]string
		taskOpts   []wasq.Option
		serverCfg  wasq.Config
		handler    func(done chan struct{}, executionCount *int32) wasq.HandlerFunc
		timeout    time.Duration
		verifyTest func(t *testing.T, count int32)
	}{
		{
			name:      "immediate task processed successfully",
			taskType:  "test:simple",
			payload:   []byte("hello wasq"),
			serverCfg: wasq.Config{Concurrency: 2},
			handler: func(done chan struct{}, executionCount *int32) wasq.HandlerFunc {
				return func(ctx context.Context, task *wasq.Task) error {
					atomic.AddInt32(executionCount, 1)
					if string(task.Payload()) != "hello wasq" {
						t.Errorf("got payload %s, want %s", task.Payload(), "hello wasq")
					}
					close(done)
					return nil
				}
			},
			timeout: 5 * time.Second,
			verifyTest: func(t *testing.T, count int32) {
				if count != 1 {
					t.Errorf("execution count = %d, want 1", count)
				}
			},
		},
		{
			name:      "task with custom headers processed",
			taskType:  "test:headers",
			payload:   []byte("header-data"),
			headers:   map[string]string{"x-correlation-id": "corr-999"},
			serverCfg: wasq.Config{Concurrency: 2},
			handler: func(done chan struct{}, executionCount *int32) wasq.HandlerFunc {
				return func(ctx context.Context, task *wasq.Task) error {
					atomic.AddInt32(executionCount, 1)
					if task.Headers()["x-correlation-id"] != "corr-999" {
						t.Errorf("got header %q, want %q", task.Headers()["x-correlation-id"], "corr-999")
					}
					close(done)
					return nil
				}
			},
			timeout: 5 * time.Second,
			verifyTest: func(t *testing.T, count int32) {
				if count != 1 {
					t.Errorf("execution count = %d, want 1", count)
				}
			},
		},
		{
			name:      "task writes result using ResultWriter",
			taskType:  "test:result",
			payload:   []byte("compute"),
			serverCfg: wasq.Config{Concurrency: 2},
			handler: func(done chan struct{}, executionCount *int32) wasq.HandlerFunc {
				return func(ctx context.Context, task *wasq.Task) error {
					atomic.AddInt32(executionCount, 1)
					n, err := task.ResultWriter().Write([]byte("result-computed-42"))
					if err != nil {
						t.Errorf("ResultWriter.Write failed: %v", err)
					}
					if n != len("result-computed-42") {
						t.Errorf("wrote %d bytes, want %d", n, len("result-computed-42"))
					}
					close(done)
					return nil
				}
			},
			timeout: 5 * time.Second,
			verifyTest: func(t *testing.T, count int32) {
				if count != 1 {
					t.Errorf("execution count = %d, want 1", count)
				}
			},
		},
		{
			name:      "task fails and retries until success",
			taskType:  "test:retry",
			payload:   []byte("retry-me"),
			taskOpts:  []wasq.Option{wasq.MaxRetry(3)},
			serverCfg: wasq.Config{
				Concurrency: 2,
				RetryDelayFunc: func(n int, e error, t *wasq.Task) time.Duration {
					return 500 * time.Millisecond
				},
			},
			handler: func(done chan struct{}, executionCount *int32) wasq.HandlerFunc {
				return func(ctx context.Context, task *wasq.Task) error {
					n := atomic.AddInt32(executionCount, 1)
					if n == 1 {
						return errors.New("transient error on first attempt")
					}
					// Second attempt succeeds
					close(done)
					return nil
				}
			},
			timeout: 10 * time.Second,
			verifyTest: func(t *testing.T, count int32) {
				if count < 1 {
					t.Errorf("expected at least 1 attempt, got %d", count)
				}
			},
		},
		{
			name:      "handler panic is caught and recovered",
			taskType:  "test:panic",
			payload:   []byte("panic-me"),
			taskOpts:  []wasq.Option{wasq.MaxRetry(1)},
			serverCfg: wasq.Config{Concurrency: 2},
			handler: func(done chan struct{}, executionCount *int32) wasq.HandlerFunc {
				return func(ctx context.Context, task *wasq.Task) error {
					atomic.AddInt32(executionCount, 1)
					close(done)
					panic("something unexpected happened in worker")
				}
			},
			timeout: 5 * time.Second,
			verifyTest: func(t *testing.T, count int32) {
				if count != 1 {
					t.Errorf("execution count = %d, want 1", count)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan struct{})
			var count int32

			server := wasq.NewServer(redisOpt, tt.serverCfg)
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
				t.Fatalf("timed out waiting for task processing after %v", tt.timeout)
			}

			server.Shutdown()

			if tt.verifyTest != nil {
				tt.verifyTest(t, atomic.LoadInt32(&count))
			}
		})
	}
}

func TestServerServeMuxRouting_TableDriven(t *testing.T) {
	redisOpt, cleanup := setupRedisContainer(t)
	defer cleanup()

	client := wasq.NewClient(redisOpt)
	defer client.Close()

	mux := wasq.NewServeMux()

	var orderHandled, emailHandled atomic.Bool
	var wg sync.WaitGroup
	wg.Add(2)

	mux.HandleFunc("order:process", func(ctx context.Context, task *wasq.Task) error {
		orderHandled.Store(true)
		wg.Done()
		return nil
	})

	mux.HandleFunc("email:send", func(ctx context.Context, task *wasq.Task) error {
		emailHandled.Store(true)
		wg.Done()
		return nil
	})

	server := wasq.NewServer(redisOpt, wasq.Config{Concurrency: 4})
	if err := server.Start(mux); err != nil {
		t.Fatalf("server.Start failed: %v", err)
	}
	defer server.Shutdown()

	if _, err := client.Enqueue(wasq.NewTask("order:process", []byte("order-1"))); err != nil {
		t.Fatalf("enqueue order:process failed: %v", err)
	}
	if _, err := client.Enqueue(wasq.NewTask("email:send", []byte("email-1"))); err != nil {
		t.Fatalf("enqueue email:send failed: %v", err)
	}

	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for mux routing")
	}

	if !orderHandled.Load() {
		t.Errorf("order:process handler was not called")
	}
	if !emailHandled.Load() {
		t.Errorf("email:send handler was not called")
	}
}
