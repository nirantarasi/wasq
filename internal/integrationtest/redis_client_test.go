package integrationtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/nirantarasi/wasq"
)

func TestClientEnqueue_TableDriven(t *testing.T) {
	redisOpt, cleanup := setupRedisContainer(t)
	defer cleanup()

	client := wasq.NewClient(redisOpt)
	defer client.Close()

	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name        string
		taskType    string
		payload     []byte
		opts        []wasq.Option
		wantQueue   string
		wantRetry   int
		wantRetain  time.Duration
		wantTimeout time.Duration
		wantHeaders map[string]string
		checkInfo   func(t *testing.T, info *wasq.TaskInfo)
	}{
		{
			name:        "immediate task with default options",
			taskType:    "email:send",
			payload:     []byte(`{"to":"user@example.com","subject":"Welcome"}`),
			opts:        nil,
			wantQueue:   "default",
			wantRetry:   25,
			wantRetain:  0,
			wantTimeout: 0,
		},
		{
			name:        "task with custom queue",
			taskType:    "image:resize",
			payload:     []byte(`{"path":"/images/avatar.png"}`),
			opts:        []wasq.Option{wasq.Queue("media")},
			wantQueue:   "media",
			wantRetry:   25,
			wantRetain:  0,
			wantTimeout: 0,
		},
		{
			name:        "task with custom max retry",
			taskType:    "payment:charge",
			payload:     []byte(`{"amount":1000}`),
			opts:        []wasq.Option{wasq.MaxRetry(3)},
			wantQueue:   "default",
			wantRetry:   3,
			wantRetain:  0,
			wantTimeout: 0,
		},
		{
			name:        "task with timeout and deadline",
			taskType:    "export:csv",
			payload:     []byte(`{"format":"csv"}`),
			opts:        []wasq.Option{wasq.Timeout(30 * time.Second), wasq.Deadline(now.Add(1 * time.Hour))},
			wantQueue:   "default",
			wantRetry:   25,
			wantTimeout: 30 * time.Second,
			checkInfo: func(t *testing.T, info *wasq.TaskInfo) {
				if info.Deadline.IsZero() {
					t.Errorf("expected non-zero deadline")
				}
			},
		},
		{
			name:       "task with retention",
			taskType:   "audit:log",
			payload:    []byte(`{"action":"login"}`),
			opts:       []wasq.Option{wasq.Retention(2 * time.Hour)},
			wantQueue:  "default",
			wantRetry:  25,
			wantRetain: 2 * time.Hour,
		},
		{
			name:      "task with custom task id and headers",
			taskType:  "notification:push",
			payload:   []byte(`{"msg":"Hello"}`),
			opts:      []wasq.Option{wasq.TaskID("task-custom-123"), wasq.Retention(10 * time.Minute)},
			wantQueue: "default",
			wantRetry: 25,
			wantHeaders: map[string]string{
				"trace_id": "trace-abc-123",
				"origin":   "auth-service",
			},
			checkInfo: func(t *testing.T, info *wasq.TaskInfo) {
				if info.ID != "task-custom-123" {
					t.Errorf("Task ID = %q, want %q", info.ID, "task-custom-123")
				}
				if diff := cmp.Diff(map[string]string{"trace_id": "trace-abc-123", "origin": "auth-service"}, info.Headers); diff != "" {
					t.Errorf("Headers mismatch (-want +got):\n%s", diff)
				}
			},
		},
		{
			name:      "task with unique lock",
			taskType:  "webhook:dispatch",
			payload:   []byte(`{"event":"order.created"}`),
			opts:      []wasq.Option{wasq.Unique(10 * time.Minute)},
			wantQueue: "default",
			wantRetry: 25,
		},
		{
			name:      "task with group aggregation",
			taskType:  "batch:item",
			payload:   []byte(`{"item_id":42}`),
			opts:      []wasq.Option{wasq.Group("daily-batch")},
			wantQueue: "default",
			wantRetry: 25,
			checkInfo: func(t *testing.T, info *wasq.TaskInfo) {
				if info.Group != "daily-batch" {
					t.Errorf("Group = %q, want %q", info.Group, "daily-batch")
				}
			},
		},
		{
			name:      "scheduled task with ProcessIn",
			taskType:  "reminder:send",
			payload:   []byte(`{"reminder_id":1}`),
			opts:      []wasq.Option{wasq.ProcessIn(10 * time.Minute)},
			wantQueue: "default",
			wantRetry: 25,
			checkInfo: func(t *testing.T, info *wasq.TaskInfo) {
				if info.NextProcessAt.Before(now.Add(9 * time.Minute)) {
					t.Errorf("NextProcessAt %v is too early, expected around 10 minutes in the future", info.NextProcessAt)
				}
			},
		},
		{
			name:      "scheduled task with ProcessAt",
			taskType:  "report:generate",
			payload:   []byte(`{"report":"monthly"}`),
			opts:      []wasq.Option{wasq.ProcessAt(now.Add(30 * time.Minute))},
			wantQueue: "default",
			wantRetry: 25,
			checkInfo: func(t *testing.T, info *wasq.TaskInfo) {
				if info.NextProcessAt.Before(now.Add(29 * time.Minute)) {
					t.Errorf("NextProcessAt %v is too early, expected around 30 minutes in the future", info.NextProcessAt)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var task *wasq.Task
			if len(tt.wantHeaders) > 0 {
				task = wasq.NewTaskWithHeaders(tt.taskType, tt.payload, tt.wantHeaders, tt.opts...)
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
				t.Errorf("info.Payload = %s, want %s", string(info.Payload), string(tt.payload))
			}
			if info.Queue != tt.wantQueue {
				t.Errorf("info.Queue = %q, want %q", info.Queue, tt.wantQueue)
			}
			if info.MaxRetry != tt.wantRetry {
				t.Errorf("info.MaxRetry = %d, want %d", info.MaxRetry, tt.wantRetry)
			}
			if tt.wantRetain > 0 && info.Retention != tt.wantRetain {
				t.Errorf("info.Retention = %v, want %v", info.Retention, tt.wantRetain)
			}
			if tt.wantTimeout > 0 && info.Timeout != tt.wantTimeout {
				t.Errorf("info.Timeout = %v, want %v", info.Timeout, tt.wantTimeout)
			}

			if tt.checkInfo != nil {
				tt.checkInfo(t, info)
			}
		})
	}
}

func TestClientBatchEnqueue_TableDriven(t *testing.T) {
	redisOpt, cleanup := setupRedisContainer(t)
	defer cleanup()

	client := wasq.NewClient(redisOpt)
	defer client.Close()

	tests := []struct {
		name       string
		tasks      []*wasq.Task
		batchOpts  []wasq.Option
		wantCount  int
		wantQueues []string
	}{
		{
			name: "batch with multiple immediate tasks",
			tasks: []*wasq.Task{
				wasq.NewTask("batch:1", []byte("1")),
				wasq.NewTask("batch:2", []byte("2")),
				wasq.NewTask("batch:3", []byte("3")),
			},
			batchOpts:  nil,
			wantCount:  3,
			wantQueues: []string{"default", "default", "default"},
		},
		{
			name: "batch with custom queue option",
			tasks: []*wasq.Task{
				wasq.NewTask("queue:1", []byte("q1")),
				wasq.NewTask("queue:2", []byte("q2")),
			},
			batchOpts:  []wasq.Option{wasq.Queue("batch_queue")},
			wantCount:  2,
			wantQueues: []string{"batch_queue", "batch_queue"},
		},
		{
			name: "batch options apply to all tasks in batch",
			tasks: []*wasq.Task{
				wasq.NewTask("mix:1", []byte("m1")),
				wasq.NewTask("mix:2", []byte("m2")),
			},
			batchOpts:  []wasq.Option{wasq.Queue("global_mix")},
			wantCount:  2,
			wantQueues: []string{"global_mix", "global_mix"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := client.BatchEnqueueContext(context.Background(), tt.tasks, tt.batchOpts...)
			if len(results) != tt.wantCount {
				t.Fatalf("got %d results, want %d", len(results), tt.wantCount)
			}

			for i, res := range results {
				if res.Err != nil {
					t.Fatalf("task[%d] enqueue failed: %v", i, res.Err)
				}
				if res.TaskInfo.Queue != tt.wantQueues[i] {
					t.Errorf("task[%d] Queue = %q, want %q", i, res.TaskInfo.Queue, tt.wantQueues[i])
				}
			}
		})
	}
}

func TestClientConflictAndValidation_TableDriven(t *testing.T) {
	redisOpt, cleanup := setupRedisContainer(t)
	defer cleanup()

	client := wasq.NewClient(redisOpt)
	defer client.Close()

	tests := []struct {
		name      string
		setup     func(c *wasq.Client)
		task      *wasq.Task
		opts      []wasq.Option
		wantErr   error
		wantPanic bool
	}{
		{
			name: "duplicate task ID returns ErrTaskIDConflict",
			setup: func(c *wasq.Client) {
				_, err := c.Enqueue(wasq.NewTask("dupe:test", []byte("first"), wasq.TaskID("unique-id-123")))
				if err != nil {
					t.Fatalf("setup enqueue failed: %v", err)
				}
			},
			task:    wasq.NewTask("dupe:test", []byte("second"), wasq.TaskID("unique-id-123")),
			wantErr: wasq.ErrTaskIDConflict,
		},
		{
			name: "unique task deduplication returns ErrDuplicateTask",
			setup: func(c *wasq.Client) {
				_, err := c.Enqueue(wasq.NewTask("unique:test", []byte("same-payload"), wasq.Unique(5*time.Minute)))
				if err != nil {
					t.Fatalf("setup unique enqueue failed: %v", err)
				}
			},
			task:    wasq.NewTask("unique:test", []byte("same-payload"), wasq.Unique(5*time.Minute)),
			wantErr: wasq.ErrDuplicateTask,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup(client)
			}

			_, err := client.Enqueue(tt.task, tt.opts...)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Enqueue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Ignore unused warning for cmpopts
var _ = cmpopts.EquateApprox(0, 0)
