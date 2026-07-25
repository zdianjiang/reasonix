package schedule

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

type fakeGateway struct {
	mu        sync.Mutex
	sentText  []ScheduledTask
	runPrompt []ScheduledTask
	errors    map[string]error
}

func (f *fakeGateway) SendText(ctx context.Context, task ScheduledTask, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentText = append(f.sentText, task)
	if f.errors != nil {
		if err := f.errors[task.ID]; err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeGateway) RunPrompt(ctx context.Context, task ScheduledTask, prompt string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runPrompt = append(f.runPrompt, task)
	if f.errors != nil {
		if err := f.errors[task.ID]; err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeGateway) counts() (text, prompt int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sentText), len(f.runPrompt)
}

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "bot-scheduler-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func TestSchedulerCreateAndTick(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	task := ScheduledTask{
		ID:            "joke-hourly",
		Title:         "Hourly joke",
		Prompt:        "Tell me a joke",
		Schedule:      "1h",
		ExecutionMode: string(ExecutionModeAIPrompt),
		Platform:      "feishu",
		ConnectionID:  "feishu",
		ChatType:      "group",
		ChatID:        "chat-123",
		UserID:        "user-123",
	}
	if err := s.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Not due yet.
	s.tick()
	if text, prompt := fg.counts(); text != 0 || prompt != 0 {
		t.Fatalf("expected no executions, got text=%d prompt=%d", text, prompt)
	}

	// Advance past the hour.
	now = now.Add(61 * time.Minute)
	s.tick()
	if text, prompt := fg.counts(); text != 0 || prompt != 1 {
		t.Fatalf("expected 1 prompt execution, got text=%d prompt=%d", text, prompt)
	}

	// Same tick should not re-run.
	s.tick()
	if text, prompt := fg.counts(); text != 0 || prompt != 1 {
		t.Fatalf("expected still 1 prompt execution, got text=%d prompt=%d", text, prompt)
	}
}

func TestSchedulerFixedTextMode(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	task := ScheduledTask{
		ID:            "fixed-greeting",
		Prompt:        "Good morning!",
		Schedule:      "30m",
		ExecutionMode: string(ExecutionModeFixedText),
		Platform:      "feishu",
		ConnectionID:  "feishu",
		ChatType:      "dm",
		ChatID:        "user-456",
		UserID:        "user-456",
	}
	if err := s.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	now = now.Add(31 * time.Minute)
	s.tick()
	if text, prompt := fg.counts(); text != 1 || prompt != 0 {
		t.Fatalf("expected 1 text execution, got text=%d prompt=%d", text, prompt)
	}
}

func TestSchedulerFailureDoesNotAdvanceRunCount(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{errors: map[string]error{"faily": errors.New("boom")}}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	task := ScheduledTask{
		ID:           "faily",
		Prompt:       "x",
		Schedule:     "1h",
		Platform:     "feishu",
		ConnectionID: "feishu",
		ChatType:     "dm",
		ChatID:       "chat",
		UserID:       "user",
	}
	if err := s.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	now = now.Add(61 * time.Minute)
	s.tick()
	if _, prompt := fg.counts(); prompt != 1 {
		t.Fatalf("expected 1 failed prompt attempt, got %d", prompt)
	}

	list := s.ListTasks(TaskFilter{})
	if len(list) != 1 {
		t.Fatalf("expected 1 task, got %d", len(list))
	}
	if list[0].RunCount != 0 {
		t.Fatalf("expected RunCount=0 after failure, got %d", list[0].RunCount)
	}
	if list[0].TriggerCount != 1 {
		t.Fatalf("expected TriggerCount=1 after failure, got %d", list[0].TriggerCount)
	}
	if list[0].FailureCount != 1 {
		t.Fatalf("expected FailureCount=1 after failure, got %d", list[0].FailureCount)
	}
	if got, want := list[0].NextRunAt, now.Add(30*time.Second).UnixMilli(); got != want {
		t.Fatalf("failure retry NextRunAt=%d, want %d", got, want)
	}
}

func TestTaskValidationRejectsSubMinuteAndInvalidCalendarRules(t *testing.T) {
	base := ScheduledTask{ID: "x", Prompt: "x", Platform: "feishu", ChatID: "chat"}
	for _, schedule := range []string{"30s", "monthly:abc@09:00", "yearly:13-1@09:00"} {
		task := base
		task.Schedule = schedule
		if err := task.Validate(); err == nil {
			t.Errorf("Validate(%q) unexpectedly succeeded", schedule)
		}
	}
}

func TestBiweeklyUsesCreationAnchor(t *testing.T) {
	created := time.Date(2026, 7, 6, 8, 0, 0, 0, time.UTC) // Monday
	task := ScheduledTask{Schedule: "biweekly:mon@09:00", CreatedAt: created.UnixMilli(), Timezone: "UTC"}
	got := computeNextRunAt(task, time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC))
	want := time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("biweekly next=%v, want %v", got, want)
	}
}

func TestSchedulerAutoPauseAfterFailures(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{errors: map[string]error{"faily": errors.New("boom")}}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	task := ScheduledTask{
		ID:           "faily",
		Prompt:       "x",
		Schedule:     "1h",
		Platform:     "feishu",
		ConnectionID: "feishu",
		ChatType:     "dm",
		ChatID:       "chat",
		UserID:       "user",
	}
	if err := s.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	for i := 0; i < maxTaskFailures; i++ {
		now = now.Add(61 * time.Minute)
		s.tick()
	}

	got, ok := s.GetTask("faily")
	if !ok {
		t.Fatal("task missing")
	}
	if got.Enabled {
		t.Fatalf("expected task to be auto-paused after %d failures, still enabled", got.FailureCount)
	}
	if got.FailureCount < maxTaskFailures {
		t.Fatalf("expected FailureCount >= %d, got %d", maxTaskFailures, got.FailureCount)
	}
}

func TestSchedulerConcurrentTasks(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	for i := 0; i < 5; i++ {
		task := ScheduledTask{
			ID:            fmt.Sprintf("task-%d", i),
			Prompt:        "x",
			Schedule:      "1h",
			ExecutionMode: string(ExecutionModeFixedText),
			Platform:      "feishu",
			ChatID:        fmt.Sprintf("chat-%d", i),
			UserID:        "user",
		}
		if err := s.CreateTask(task); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}

	now = now.Add(61 * time.Minute)
	s.tick()

	if text, _ := fg.counts(); text != 5 {
		t.Fatalf("expected 5 text executions, got %d", text)
	}
}

func TestSchedulersSharingStateDoNotDuplicateExecution(t *testing.T) {
	dir := tempDir(t)
	t.Setenv("REASONIX_STATE_HOME", dir)

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}
	s1, s2 := New(fg), New(fg)
	for _, s := range []*Scheduler{s1, s2} {
		s.SetNowFunc(func() time.Time { return now })
		s.Start()
		defer s.Stop()
	}
	if err := s1.CreateTask(ScheduledTask{
		ID: "shared", Prompt: "x", Schedule: "1h", ExecutionMode: string(ExecutionModeFixedText),
		Platform: "feishu", ChatID: "chat", UserID: "user",
	}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	now = now.Add(61 * time.Minute)

	var wg sync.WaitGroup
	for _, s := range []*Scheduler{s1, s2} {
		wg.Add(1)
		go func(s *Scheduler) { defer wg.Done(); s.tick() }(s)
	}
	wg.Wait()
	if text, _ := fg.counts(); text != 1 {
		t.Fatalf("shared-state executions=%d, want 1", text)
	}
}

func TestSchedulerPersistence(t *testing.T) {
	dir := tempDir(t)
	t.Setenv("REASONIX_STATE_HOME", dir)

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}

	// First scheduler instance creates the task.
	s1 := New(fg)
	s1.SetNowFunc(func() time.Time { return now })
	s1.Start()
	task := ScheduledTask{
		ID:           "persisted",
		Prompt:       "hi",
		Schedule:     "1h",
		Platform:     "feishu",
		ConnectionID: "feishu",
		ChatType:     "dm",
		ChatID:       "chat",
		UserID:       "user",
	}
	if err := s1.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	s1.Stop()

	// Second scheduler instance loads the task.
	s2 := New(fg)
	s2.SetNowFunc(func() time.Time { return now.Add(61 * time.Minute) })
	s2.Start()
	defer s2.Stop()

	s2.tick()
	if _, prompt := fg.counts(); prompt != 1 {
		t.Fatalf("expected persisted task to run, got %d prompt executions", prompt)
	}
}

func TestSchedulerCancel(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	task := ScheduledTask{
		ID:           "cancel-me",
		Prompt:       "x",
		Schedule:     "1h",
		Platform:     "feishu",
		ConnectionID: "feishu",
		ChatType:     "dm",
		ChatID:       "chat",
		UserID:       "user",
	}
	if err := s.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := s.CancelTask("cancel-me"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	now = now.Add(61 * time.Minute)
	s.tick()
	if _, prompt := fg.counts(); prompt != 0 {
		t.Error("expected cancelled task not to run")
	}
}

func TestSchedulerListFilter(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	fg := &fakeGateway{}
	s := New(fg)
	s.Start()
	defer s.Stop()

	for _, task := range []ScheduledTask{
		{ID: "a", Prompt: "x", Schedule: "1h", Platform: "feishu", ChatID: "chat1", UserID: "u1"},
		{ID: "b", Prompt: "x", Schedule: "1h", Platform: "feishu", ChatID: "chat2", UserID: "u2"},
		{ID: "c", Prompt: "x", Schedule: "1h", Platform: "qq", ChatID: "chat3", UserID: "u1"},
	} {
		if err := s.CreateTask(task); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}

	if got := len(s.ListTasks(TaskFilter{Platform: "feishu"})); got != 2 {
		t.Fatalf("expected 2 feishu tasks, got %d", got)
	}
	if got := len(s.ListTasks(TaskFilter{UserID: "u1"})); got != 2 {
		t.Fatalf("expected 2 tasks for u1, got %d", got)
	}
	if got := len(s.ListTasks(TaskFilter{ChatID: "chat2"})); got != 1 {
		t.Fatalf("expected 1 task for chat2, got %d", got)
	}
}

func TestSchedulerAIPromptTracksSuccessfulRun(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	task := ScheduledTask{
		ID:            "ai",
		Prompt:        "hi",
		Schedule:      "1h",
		ExecutionMode: string(ExecutionModeAIPrompt),
		Platform:      "feishu",
		ChatID:        "chat",
		UserID:        "user",
	}
	if err := s.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	now = now.Add(61 * time.Minute)
	s.tick()

	got, ok := s.GetTask("ai")
	if !ok {
		t.Fatal("task missing")
	}
	if got.TriggerCount != 1 {
		t.Fatalf("TriggerCount=%d, want 1", got.TriggerCount)
	}
	if got.RunCount != 1 {
		t.Fatalf("RunCount=%d, want 1 for successful ai_prompt", got.RunCount)
	}
}

func TestSchedulerAIPromptHonorsMaxRuns(t *testing.T) {
	t.Setenv("REASONIX_STATE_HOME", tempDir(t))

	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	fg := &fakeGateway{}
	s := New(fg)
	s.SetNowFunc(func() time.Time { return now })
	s.Start()
	defer s.Stop()

	if err := s.CreateTask(ScheduledTask{
		ID: "ai-one-shot", Prompt: "hi", Schedule: "1h", MaxRuns: 1,
		ExecutionMode: string(ExecutionModeAIPrompt), Platform: "feishu", ChatID: "chat", UserID: "user",
	}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	now = now.Add(61 * time.Minute)
	s.tick()
	now = now.Add(2 * time.Hour)
	s.tick()

	if _, prompt := fg.counts(); prompt != 1 {
		t.Fatalf("AI prompt executions=%d, want 1", prompt)
	}
	got, ok := s.GetTask("ai-one-shot")
	if !ok || got.RunCount != 1 {
		t.Fatalf("task after max_runs = %+v, found=%v", got, ok)
	}
}
