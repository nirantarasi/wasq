package integrationtest

import (
	"testing"
	"time"

	"github.com/nirantarasi/wasq"
)

func TestInspectorQueueOperations_TableDriven(t *testing.T) {
	redisOpt, cleanup := setupRedisContainer(t)
	defer cleanup()

	client := wasq.NewClient(redisOpt)
	defer client.Close()

	inspector := wasq.NewInspector(redisOpt)
	defer inspector.Close()

	tests := []struct {
		name       string
		setup      func(t *testing.T)
		action     func(t *testing.T)
		verifyStep func(t *testing.T)
	}{
		{
			name: "inspect queue info reflects enqueued pending tasks",
			setup: func(t *testing.T) {
				for i := 0; i < 3; i++ {
					_, err := client.Enqueue(wasq.NewTask("inspect:test", []byte("1"), wasq.Queue("inspect_q")))
					if err != nil {
						t.Fatalf("enqueue failed: %v", err)
					}
				}
			},
			action: func(t *testing.T) {},
			verifyStep: func(t *testing.T) {
				info, err := inspector.GetQueueInfo("inspect_q")
				if err != nil {
					t.Fatalf("GetQueueInfo failed: %v", err)
				}
				if info.Pending != 3 {
					t.Errorf("Pending = %d, want 3", info.Pending)
				}
				if info.Paused {
					t.Errorf("expected queue not to be paused")
				}
			},
		},
		{
			name: "pause queue sets paused status to true",
			setup: func(t *testing.T) {
				_, err := client.Enqueue(wasq.NewTask("pause:test", []byte("1"), wasq.Queue("pause_q")))
				if err != nil {
					t.Fatalf("enqueue failed: %v", err)
				}
			},
			action: func(t *testing.T) {
				if err := inspector.PauseQueue("pause_q"); err != nil {
					t.Fatalf("PauseQueue failed: %v", err)
				}
			},
			verifyStep: func(t *testing.T) {
				info, err := inspector.GetQueueInfo("pause_q")
				if err != nil {
					t.Fatalf("GetQueueInfo failed: %v", err)
				}
				if !info.Paused {
					t.Errorf("expected queue to be paused")
				}
			},
		},
		{
			name: "unpause queue resumes paused queue",
			setup: func(t *testing.T) {
				_, err := client.Enqueue(wasq.NewTask("resume:test", []byte("1"), wasq.Queue("resume_q")))
				if err != nil {
					t.Fatalf("enqueue failed: %v", err)
				}
				if err := inspector.PauseQueue("resume_q"); err != nil {
					t.Fatalf("PauseQueue failed: %v", err)
				}
			},
			action: func(t *testing.T) {
				if err := inspector.UnpauseQueue("resume_q"); err != nil {
					t.Fatalf("UnpauseQueue failed: %v", err)
				}
			},
			verifyStep: func(t *testing.T) {
				info, err := inspector.GetQueueInfo("resume_q")
				if err != nil {
					t.Fatalf("GetQueueInfo failed: %v", err)
				}
				if info.Paused {
					t.Errorf("expected queue not to be paused after unpause")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup(t)
			}
			if tt.action != nil {
				tt.action(t)
			}
			if tt.verifyStep != nil {
				tt.verifyStep(t)
			}
		})
	}
}

func TestInspectorTaskOperations_TableDriven(t *testing.T) {
	redisOpt, cleanup := setupRedisContainer(t)
	defer cleanup()

	client := wasq.NewClient(redisOpt)
	defer client.Close()

	inspector := wasq.NewInspector(redisOpt)
	defer inspector.Close()

	tests := []struct {
		name       string
		setup      func(t *testing.T) (taskID string)
		operation  func(t *testing.T, taskID string)
		verifyStep func(t *testing.T, taskID string)
	}{
		{
			name: "delete pending task removes it from queue",
			setup: func(t *testing.T) string {
				info, err := client.Enqueue(wasq.NewTask("task:delete", []byte("delete-me"), wasq.Queue("ops_q")))
				if err != nil {
					t.Fatalf("enqueue failed: %v", err)
				}
				return info.ID
			},
			operation: func(t *testing.T, taskID string) {
				if err := inspector.DeleteTask("ops_q", taskID); err != nil {
					t.Fatalf("DeleteTask failed: %v", err)
				}
			},
			verifyStep: func(t *testing.T, taskID string) {
				_, err := inspector.GetTaskInfo("ops_q", taskID)
				if err == nil {
					t.Fatalf("expected error getting deleted task, got nil")
				}
			},
		},
		{
			name: "run scheduled task moves it to pending immediately",
			setup: func(t *testing.T) string {
				info, err := client.Enqueue(
					wasq.NewTask("task:scheduled", []byte("run-early"), wasq.Queue("ops_q"), wasq.ProcessIn(1*time.Hour)),
				)
				if err != nil {
					t.Fatalf("enqueue failed: %v", err)
				}
				return info.ID
			},
			operation: func(t *testing.T, taskID string) {
				if err := inspector.RunTask("ops_q", taskID); err != nil {
					t.Fatalf("RunTask failed: %v", err)
				}
			},
			verifyStep: func(t *testing.T, taskID string) {
				tasks, err := inspector.ListPendingTasks("ops_q")
				if err != nil {
					t.Fatalf("ListPendingTasks failed: %v", err)
				}
				found := false
				for _, tsk := range tasks {
					if tsk.ID == taskID {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("task %q was not found in pending tasks after RunTask", taskID)
				}
			},
		},
		{
			name: "archive pending task moves it to archived set",
			setup: func(t *testing.T) string {
				info, err := client.Enqueue(wasq.NewTask("task:archive", []byte("archive-me"), wasq.Queue("ops_q")))
				if err != nil {
					t.Fatalf("enqueue failed: %v", err)
				}
				return info.ID
			},
			operation: func(t *testing.T, taskID string) {
				if err := inspector.ArchiveTask("ops_q", taskID); err != nil {
					t.Fatalf("ArchiveTask failed: %v", err)
				}
			},
			verifyStep: func(t *testing.T, taskID string) {
				tasks, err := inspector.ListArchivedTasks("ops_q")
				if err != nil {
					t.Fatalf("ListArchivedTasks failed: %v", err)
				}
				found := false
				for _, tsk := range tasks {
					if tsk.ID == taskID {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("task %q was not found in archived tasks after ArchiveTask", taskID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskID := tt.setup(t)
			tt.operation(t, taskID)
			tt.verifyStep(t, taskID)
		})
	}
}
