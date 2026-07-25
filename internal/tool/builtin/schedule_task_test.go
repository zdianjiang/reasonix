package builtin

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	scheduler "reasonix/internal/schedule"
)

func TestScheduleTaskToolCreatesTask(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", t.TempDir())

	gw := &fakeSchedulerGateway{}
	s := scheduler.New(gw)
	s.Start()
	defer s.Stop()

	ctx := context.Background()
	ctx = scheduler.WithScheduler(ctx, s)
	ctx = scheduler.WithChatContext(ctx, scheduler.ChatContext{
		Platform:     "feishu",
		ConnectionID: "feishu",
		ChatType:     "group",
		ChatID:       "chat-123",
		UserID:       "user-123",
	})

	tool := scheduleTaskTool{}
	args, _ := json.Marshal(map[string]any{
		"prompt":   "每小时讲一个笑话",
		"schedule": "1h",
	})
	result, err := tool.Execute(ctx, args)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result == "" {
		t.Fatal("expected non-empty confirmation")
	}

	tasks := s.ListTasks(scheduler.TaskFilter{})
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Prompt != "每小时讲一个笑话" {
		t.Errorf("prompt = %q, want %q", tasks[0].Prompt, "每小时讲一个笑话")
	}
	if tasks[0].Platform != "feishu" {
		t.Errorf("platform = %q, want feishu", tasks[0].Platform)
	}
}

func TestScheduleTaskToolRequiresChatContext(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", t.TempDir())

	gw := &fakeSchedulerGateway{}
	s := scheduler.New(gw)

	ctx := context.Background()
	ctx = scheduler.WithScheduler(ctx, s)

	tool := scheduleTaskTool{}
	args, _ := json.Marshal(map[string]any{
		"prompt":   "x",
		"schedule": "1h",
	})
	if _, err := tool.Execute(ctx, args); err == nil {
		t.Fatal("expected error without chat context")
	}
}

func TestScheduleToolsRejectScheduledExecution(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", t.TempDir())
	s := scheduler.New(fakeSchedulerGateway{})
	ctx := scheduler.WithScheduler(context.Background(), s)
	ctx = scheduler.WithChatContext(ctx, scheduler.ChatContext{Platform: "feishu", ChatID: "chat", UserID: "user", Scheduled: true})
	args := json.RawMessage(`{"prompt":"x","schedule":"1h"}`)
	if _, err := (scheduleTaskTool{}).Execute(ctx, args); err == nil {
		t.Fatal("scheduled execution created a recurring task")
	}
}

func TestScheduleManagementTools(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", t.TempDir())

	s := scheduler.New(fakeSchedulerGateway{})
	s.Start()
	defer s.Stop()

	ctx := scheduler.WithScheduler(context.Background(), s)
	ctx = scheduler.WithChatContext(ctx, scheduler.ChatContext{
		Platform:     "feishu",
		ConnectionID: "feishu",
		ChatID:       "chat-123",
		UserID:       "user-123",
	})

	createArgs, _ := json.Marshal(map[string]any{
		"prompt":   "日报",
		"schedule": "1h",
	})
	if _, err := (scheduleTaskTool{}).Execute(ctx, createArgs); err != nil {
		t.Fatalf("create Execute: %v", err)
	}

	tasks := s.ListTasks(scheduler.TaskFilter{})
	if len(tasks) != 1 {
		t.Fatalf("tasks=%d, want 1", len(tasks))
	}
	taskID := tasks[0].ID

	listOut, err := (scheduleListTool{}).Execute(ctx, []byte(`{}`))
	if err != nil || !strings.Contains(listOut, taskID) {
		t.Fatalf("list err=%v out=%q", err, listOut)
	}
	if !strings.Contains(listOut, "runs=0") || !strings.Contains(listOut, "last=never") {
		t.Fatalf("list must expose execution state: %q", listOut)
	}

	// Pause should keep the task but disable it.
	pauseArg, _ := json.Marshal(map[string]any{"task_id": taskID})
	if _, err := (schedulePauseTool{}).Execute(ctx, pauseArg); err != nil {
		t.Fatalf("pause Execute: %v", err)
	}
	paused, ok := s.GetTask(taskID)
	if !ok {
		t.Fatal("task missing after pause")
	}
	if paused.Enabled {
		t.Fatal("task should be disabled after pause")
	}

	// Resume should re-enable it.
	if _, err := (scheduleResumeTool{}).Execute(ctx, pauseArg); err != nil {
		t.Fatalf("resume Execute: %v", err)
	}
	resumed, ok := s.GetTask(taskID)
	if !ok {
		t.Fatal("task missing after resume")
	}
	if !resumed.Enabled {
		t.Fatal("task should be enabled after resume")
	}

	// Delete should remove the task.
	if _, err := (scheduleDeleteTool{}).Execute(ctx, pauseArg); err != nil {
		t.Fatalf("delete Execute: %v", err)
	}
	if _, ok := s.GetTask(taskID); ok {
		t.Fatal("task should be deleted")
	}
}

type fakeSchedulerGateway struct{}

func (fakeSchedulerGateway) SendText(ctx context.Context, task scheduler.ScheduledTask, text string) error {
	return nil
}

func (fakeSchedulerGateway) RunPrompt(ctx context.Context, task scheduler.ScheduledTask, prompt string) error {
	return nil
}

// silence unused imports in case testing/time/os are not referenced elsewhere
var _ = os.ReadFile
var _ = time.Now
