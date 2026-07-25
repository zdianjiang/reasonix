// Package scheduler runs persistent, chat-bound scheduled tasks for the bot.
// It is intentionally decoupled from the concrete bot gateway: the gateway
// provides a thin adapter implementing the Gateway interface, while this
// package owns persistence, scheduling, and execution logic.
package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"reasonix/internal/config"
)

// ExecutionMode controls how a scheduled task runs when it fires.
type ExecutionMode string

const (
	ExecutorBotMessage = "bot_message"
	ExecutorBotAgent   = "bot_agent"
	// ExecutionModeAIPrompt runs the prompt through the AI each time.
	ExecutionModeAIPrompt ExecutionMode = "ai_prompt"
	// ExecutionModeFixedText sends the prompt text verbatim.
	ExecutionModeFixedText ExecutionMode = "fixed_text"
)

// ScheduledTask is one recurring task bound to a specific chat.
type ScheduledTask struct {
	ID            string `json:"id"`
	Title         string `json:"title,omitempty"`
	Prompt        string `json:"prompt"`
	Schedule      string `json:"schedule"`
	Enabled       bool   `json:"enabled"`
	ExecutionMode string `json:"execution_mode"`
	// ExecutorKind selects the delivery backend. Empty values are migrated from
	// ExecutionMode for compatibility with early Bot-only tasks.
	ExecutorKind  string `json:"executor_kind,omitempty"`
	LastTriggerAt int64  `json:"last_trigger_at,omitempty"`
	TriggerCount  int    `json:"trigger_count,omitempty"`

	// Chat context captured when the task was created.
	Platform     string `json:"platform"`
	ConnectionID string `json:"connection_id"`
	Domain       string `json:"domain,omitempty"`
	ChatType     string `json:"chat_type"`
	ChatID       string `json:"chat_id"`
	UserID       string `json:"user_id,omitempty"`
	ThreadID     string `json:"thread_id,omitempty"`

	CreatedAt    int64  `json:"created_at"`
	LastRunAt    int64  `json:"last_run_at,omitempty"`
	NextRunAt    int64  `json:"next_run_at,omitempty"`
	RunCount     int    `json:"run_count,omitempty"`
	MaxRuns      int    `json:"max_runs,omitempty"`
	FailureCount int    `json:"failure_count,omitempty"`
	LastFailedAt int64  `json:"last_failed_at,omitempty"`
	LastError    string `json:"last_error,omitempty"`
	PausedReason string `json:"paused_reason,omitempty"`
	Timezone     string `json:"timezone,omitempty"`
}

// Validate checks required fields and normalizes execution mode.
func (t *ScheduledTask) Validate() error {
	if strings.TrimSpace(t.ID) == "" {
		return errors.New("scheduled task id is required")
	}
	if strings.TrimSpace(t.Prompt) == "" {
		return errors.New("scheduled task prompt is required")
	}
	if strings.TrimSpace(t.Schedule) == "" {
		return errors.New("scheduled task schedule is required")
	}
	if strings.TrimSpace(t.ChatID) == "" {
		return errors.New("scheduled task chat_id is required")
	}
	if strings.TrimSpace(t.Platform) == "" {
		return errors.New("scheduled task platform is required")
	}
	if t.MaxRuns < 0 {
		return errors.New("max_runs must not be negative")
	}
	if tz := strings.TrimSpace(t.Timezone); tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return fmt.Errorf("invalid timezone %q", tz)
		}
	}
	switch ExecutionMode(strings.TrimSpace(t.ExecutionMode)) {
	case "", ExecutionModeAIPrompt, ExecutionModeFixedText:
		// ok
	default:
		return fmt.Errorf("unsupported execution_mode: %q", t.ExecutionMode)
	}
	if _, ok := parseTaskSchedule(t.Schedule); !ok {
		if d, err := parseInterval(t.Schedule); err != nil || d < time.Minute {
			return fmt.Errorf("invalid schedule: %q", t.Schedule)
		}
	}
	return nil
}

// EffectiveExecutionMode returns the concrete execution mode, defaulting to AI.
func (t ScheduledTask) EffectiveExecutionMode() ExecutionMode {
	m := ExecutionMode(strings.TrimSpace(t.ExecutionMode))
	if m == "" || (m != ExecutionModeAIPrompt && m != ExecutionModeFixedText) {
		return ExecutionModeAIPrompt
	}
	return m
}

// IsOwner reports whether the given user is the task creator.
func (t ScheduledTask) IsOwner(userID string) bool {
	return strings.TrimSpace(t.UserID) == strings.TrimSpace(userID)
}

// taskFile is the on-disk format.
type taskFile struct {
	Version int             `json:"version"`
	Tasks   []ScheduledTask `json:"tasks"`
}

const taskFileVersion = 1

const maxTasksPerChat = 50

// Scheduler runs scheduled bot tasks and persists them to disk.
type Scheduler struct {
	executors map[string]Executor
	mu        sync.Mutex
	tasks     []ScheduledTask
	cfgMod    time.Time
	done      chan struct{}
	running   bool
	runCtx    context.Context
	cancel    context.CancelFunc
	now       func() time.Time // injectable for tests
}

// Executor executes one durable schedule task. Bot, desktop and future webhook
// integrations register their own executors; the scheduler owns no UI/channel
// dependency.
type Executor interface {
	Kind() string
	Execute(context.Context, ScheduledTask) error
}

// Gateway is the temporary compatibility surface for the original Bot-only
// constructor. New application integrations should use RegisterExecutor.
type Gateway interface {
	// SendText sends a fixed-text message to the chat described by task.
	SendText(ctx context.Context, task ScheduledTask, text string) error
	// RunPrompt asks the bot to run a prompt in the chat described by task.
	RunPrompt(ctx context.Context, task ScheduledTask, prompt string) error
}

// New creates a scheduler bound to the given gateway adapter.
func New(gateways ...Gateway) *Scheduler {
	s := &Scheduler{executors: make(map[string]Executor), now: time.Now}
	for _, gateway := range gateways {
		if gateway != nil {
			s.RegisterExecutor(botMessageExecutor{gateway})
			s.RegisterExecutor(botAgentExecutor{gateway})
		}
	}
	return s
}

func (s *Scheduler) RegisterExecutor(executor Executor) {
	if executor == nil || strings.TrimSpace(executor.Kind()) == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executors[executor.Kind()] = executor
}

func (t ScheduledTask) EffectiveExecutorKind() string {
	if kind := strings.TrimSpace(t.ExecutorKind); kind != "" {
		return kind
	}
	if t.EffectiveExecutionMode() == ExecutionModeFixedText {
		return ExecutorBotMessage
	}
	return ExecutorBotAgent
}

type botMessageExecutor struct{ gateway Gateway }

func (botMessageExecutor) Kind() string { return ExecutorBotMessage }
func (e botMessageExecutor) Execute(ctx context.Context, task ScheduledTask) error {
	return e.gateway.SendText(ctx, task, task.Prompt)
}

type botAgentExecutor struct{ gateway Gateway }

func (botAgentExecutor) Kind() string { return ExecutorBotAgent }
func (e botAgentExecutor) Execute(ctx context.Context, task ScheduledTask) error {
	return e.gateway.RunPrompt(ctx, task, task.Prompt)
}

// configPath returns the JSON file path.
func (s *Scheduler) configPath() string {
	dir := config.MemoryUserDir()
	if dir == "" {
		dir = "."
	}
	return filepath.Join(dir, "bot-scheduled-tasks.db")
}

// loadTasks reads tasks from disk.
func (s *Scheduler) loadTasks() []ScheduledTask {
	// One-time migration from the pre-SQLite JSON store. Only attempt it before
	// the database exists, so an intentionally empty database never resurrects
	// deleted tasks from an old file.
	_, dbErr := os.Stat(s.configPath())
	if os.IsNotExist(dbErr) {
		legacy := filepath.Join(filepath.Dir(s.configPath()), "bot-scheduled-tasks.json")
		if raw, err := os.ReadFile(legacy); err == nil {
			var old taskFile
			if err := json.Unmarshal(raw, &old); err == nil && len(old.Tasks) > 0 {
				if err := (sqliteStore{path: s.configPath()}).replace(old.Tasks, s.now().UnixMilli()); err != nil {
					log.Printf("[bot-scheduler] migrate legacy tasks failed: %v", err)
				} else {
					log.Printf("[bot-scheduler] migrated %d legacy tasks to SQLite", len(old.Tasks))
				}
			}
		}
	}
	tasks, err := (sqliteStore{path: s.configPath()}).load()
	if err != nil {
		log.Printf("[bot-scheduler] load tasks failed: %v", err)
		return nil
	}
	return tasks
}

// saveTasks writes tasks to disk atomically.
func (s *Scheduler) saveTasks(tasks []ScheduledTask) error {
	if tasks == nil {
		tasks = []ScheduledTask{}
	}
	return (sqliteStore{path: s.configPath()}).replace(tasks, s.now().UnixMilli())
}

// noteConfigModLocked records the config file mtime after a write.
func (s *Scheduler) noteConfigModLocked() {
	// SQLite is read on each public operation; no mtime cache is needed.
}

// adoptExternalEditsLocked re-reads the file if its mtime changed.
func (s *Scheduler) adoptExternalEditsLocked() {
	s.tasks = s.loadTasks()
	s.sanitizeLoadedTasksLocked()
}

func (s *Scheduler) sanitizeLoadedTasksLocked() {
	for i := range s.tasks {
		if err := s.tasks[i].Validate(); err != nil {
			s.tasks[i].Enabled = false
			s.tasks[i].PausedReason = "invalid persisted task: " + err.Error()
			log.Printf("[bot-scheduler] disabled invalid task %q: %v", s.tasks[i].ID, err)
		}
	}
}

// Start launches the scheduler goroutine.
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}
	s.done = make(chan struct{})
	s.runCtx, s.cancel = context.WithCancel(context.Background())
	s.tasks = s.loadTasks()
	s.sanitizeLoadedTasksLocked()
	for i := range s.tasks {
		if s.tasks[i].NextRunAt == 0 {
			s.tasks[i].NextRunAt = computeNextRunAt(s.tasks[i], s.now()).UnixMilli()
		}
	}
	s.noteConfigModLocked()
	s.running = true
	go s.loop()
	log.Printf("[bot-scheduler] started (%d tasks)", len(s.tasks))
}

// Stop signals the scheduler goroutine to exit.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.done)
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
}

// Running reports whether the scheduler loop is active.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

func (s *Scheduler) loop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

func (s *Scheduler) tick() {
	unlock, acquired, err := tryExecutionLock(s.configPath() + ".run.lock")
	if err != nil {
		log.Printf("[bot-scheduler] acquire execution lock failed: %v", err)
		return
	}
	if !acquired {
		// Another bot process owns this state directory and is running the due
		// set. Leave it to that process rather than delivering duplicates.
		return
	}
	defer unlock()

	s.mu.Lock()
	s.adoptExternalEditsLocked()
	tasks := append([]ScheduledTask(nil), s.tasks...)
	s.mu.Unlock()

	now := s.now()
	var due []ScheduledTask
	for _, t := range tasks {
		if !t.Enabled {
			continue
		}
		if t.MaxRuns > 0 && t.RunCount >= t.MaxRuns {
			continue
		}
		if !taskDueAt(t, now) {
			continue
		}
		due = append(due, t)
	}
	if len(due) == 0 {
		return
	}

	// Run each due task concurrently so a long AI turn cannot block the
	// scheduler loop or other tasks.
	results := make(chan ScheduledTask, len(due))
	var wg sync.WaitGroup
	for _, t := range due {
		wg.Add(1)
		go func(task ScheduledTask) {
			defer wg.Done()
			results <- s.executeTask(task)
		}(t)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	updates := make(map[string]ScheduledTask)
	for t := range results {
		updates[t.ID] = t
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mergeRunUpdatesLocked(updates)
}

const maxTaskFailures = 3

func (s *Scheduler) executeTask(t ScheduledTask) ScheduledTask {
	s.mu.Lock()
	parent := s.runCtx
	s.mu.Unlock()
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()

	kind := t.EffectiveExecutorKind()
	now := s.now()
	t.LastTriggerAt = now.UnixMilli()
	t.TriggerCount++

	s.mu.Lock()
	executor := s.executors[kind]
	s.mu.Unlock()
	var err error
	if executor == nil {
		err = fmt.Errorf("no executor registered for %q", kind)
	} else {
		err = executor.Execute(ctx, t)
	}

	if err != nil {
		log.Printf("[bot-scheduler] task %q failed: %v", t.ID, err)
		t.FailureCount++
		t.LastFailedAt = now.UnixMilli()
		t.LastError = err.Error()
		if t.FailureCount >= maxTaskFailures {
			log.Printf("[bot-scheduler] task %q paused after %d consecutive failures", t.ID, t.FailureCount)
			t.Enabled = false
			t.PausedReason = "repeated failures"
		}
		// Retries deliberately use a direct backoff rather than the recurrence
		// calculation (which could otherwise postpone a daily task for a day).
		t.NextRunAt = now.Add(30 * time.Second).UnixMilli()
		return t
	}

	// A successful AI turn is an execution just as a successful fixed-text
	// delivery is.  Keeping both modes on the same accounting path is required
	// for MaxRuns to cap recurring AI tasks as advertised.
	finished := s.now()
	t.LastRunAt = finished.UnixMilli()
	t.RunCount++
	t.FailureCount = 0
	t.LastFailedAt = 0
	t.LastError = ""
	t.PausedReason = ""
	// Fixed-delay semantics avoid a long AI turn immediately triggering a
	// catch-up run when its configured period is shorter than its duration.
	t.NextRunAt = computeNextRunAt(t, finished).UnixMilli()
	return t
}

func (s *Scheduler) mergeRunUpdatesLocked(updates map[string]ScheduledTask) {
	if len(updates) == 0 {
		return
	}
	tasks := s.loadTasks()
	if tasks == nil {
		tasks = append([]ScheduledTask(nil), s.tasks...)
	}
	for i := range tasks {
		u, ok := updates[tasks[i].ID]
		if !ok {
			continue
		}
		tasks[i].LastRunAt = u.LastRunAt
		tasks[i].LastTriggerAt = u.LastTriggerAt
		tasks[i].NextRunAt = u.NextRunAt
		tasks[i].RunCount = u.RunCount
		tasks[i].TriggerCount = u.TriggerCount
		tasks[i].FailureCount = u.FailureCount
		tasks[i].LastFailedAt = u.LastFailedAt
		tasks[i].LastError = u.LastError
		tasks[i].PausedReason = u.PausedReason
		tasks[i].Enabled = u.Enabled
	}
	s.tasks = tasks
	_ = s.saveTasks(tasks)
	s.noteConfigModLocked()
}

// CreateTask adds a new task and persists it.
func (s *Scheduler) CreateTask(t ScheduledTask) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adoptExternalEditsLocked()
	for _, existing := range s.tasks {
		if existing.ID == t.ID {
			return fmt.Errorf("task %q already exists", t.ID)
		}
	}
	count := 0
	for _, existing := range s.tasks {
		if existing.Platform == t.Platform && existing.ConnectionID == t.ConnectionID && existing.ChatID == t.ChatID {
			count++
		}
	}
	if count >= maxTasksPerChat {
		return fmt.Errorf("task limit reached for this chat (%d)", maxTasksPerChat)
	}
	if t.CreatedAt == 0 {
		t.CreatedAt = s.now().UnixMilli()
	}
	t.Enabled = true
	t.PausedReason = ""
	t.NextRunAt = computeNextRunAt(t, s.now()).UnixMilli()
	s.tasks = append(s.tasks, t)
	if err := s.saveTasks(s.tasks); err != nil {
		s.tasks = s.tasks[:len(s.tasks)-1]
		return err
	}
	s.noteConfigModLocked()
	return nil
}

// GetTask returns a single task by id.
func (s *Scheduler) GetTask(id string) (ScheduledTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adoptExternalEditsLocked()
	for _, t := range s.tasks {
		if t.ID == id {
			return t, true
		}
	}
	return ScheduledTask{}, false
}

// ListTasks returns a filtered copy of tasks.
func (s *Scheduler) ListTasks(filter TaskFilter) []ScheduledTask {
	s.mu.Lock()
	s.adoptExternalEditsLocked()
	tasks := append([]ScheduledTask(nil), s.tasks...)
	s.mu.Unlock()

	out := make([]ScheduledTask, 0, len(tasks))
	for _, t := range tasks {
		if filter.Platform != "" && t.Platform != filter.Platform {
			continue
		}
		if filter.ConnectionID != "" && t.ConnectionID != filter.ConnectionID {
			continue
		}
		if filter.ChatID != "" && t.ChatID != filter.ChatID {
			continue
		}
		if filter.UserID != "" && !t.IsOwner(filter.UserID) {
			continue
		}
		if filter.EnabledOnly && !t.Enabled {
			continue
		}
		out = append(out, t)
	}
	return out
}

// TaskFilter selects tasks for ListTasks.
type TaskFilter struct {
	Platform     string
	ConnectionID string
	ChatID       string
	UserID       string
	EnabledOnly  bool
}

func (s *Scheduler) updateTask(id string, mutate func(ScheduledTask) (ScheduledTask, bool)) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("task id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adoptExternalEditsLocked()
	for i := range s.tasks {
		if s.tasks[i].ID != id {
			continue
		}
		updated, changed := mutate(s.tasks[i])
		if !changed {
			return nil
		}
		s.tasks[i] = updated
		if err := s.saveTasks(s.tasks); err != nil {
			return err
		}
		s.noteConfigModLocked()
		return nil
	}
	return fmt.Errorf("task %q not found", id)
}

// DeleteTask removes a task by id.
func (s *Scheduler) DeleteTask(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("task id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adoptExternalEditsLocked()
	for i := range s.tasks {
		if s.tasks[i].ID != id {
			continue
		}
		s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
		if err := s.saveTasks(s.tasks); err != nil {
			return err
		}
		s.noteConfigModLocked()
		return nil
	}
	return fmt.Errorf("task %q not found", id)
}

// CancelTask removes a task by id.
func (s *Scheduler) CancelTask(id string) error { return s.DeleteTask(id) }

// PauseTask disables a task without deleting it.
func (s *Scheduler) PauseTask(id string) error {
	return s.updateTask(id, func(t ScheduledTask) (ScheduledTask, bool) {
		if t.Enabled {
			t.Enabled = false
			t.PausedReason = "paused by user"
			return t, true
		}
		return t, false
	})
}

// ResumeTask re-enables a task and recomputes its next run.
func (s *Scheduler) ResumeTask(id string) error {
	return s.updateTask(id, func(t ScheduledTask) (ScheduledTask, bool) {
		if !t.Enabled {
			t.Enabled = true
			t.PausedReason = ""
			t.NextRunAt = computeNextRunAt(t, s.now()).UnixMilli()
			return t, true
		}
		return t, false
	})
}

// SetNowFunc is test-only: replaces time.Now.
func (s *Scheduler) SetNowFunc(fn func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = fn
}
