package builtin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	scheduler "reasonix/internal/schedule"
	"reasonix/internal/tool"
)

func init() {
	tool.RegisterBuiltin(&scheduleTaskTool{})
	tool.RegisterBuiltin(&scheduleListTool{})
	tool.RegisterBuiltin(&scheduleCancelTool{})
	tool.RegisterBuiltin(&scheduleDeleteTool{})
	tool.RegisterBuiltin(&schedulePauseTool{})
	tool.RegisterBuiltin(&scheduleResumeTool{})
}

type scheduleTaskArgs struct {
	Prompt        string `json:"prompt"`
	Schedule      string `json:"schedule"`
	ExecutionMode string `json:"execution_mode,omitempty"`
	Title         string `json:"title,omitempty"`
	MaxRuns       int    `json:"max_runs,omitempty"`
}

type scheduleTaskTool struct{}

func (scheduleTaskTool) Name() string { return "schedule_task" }

func (scheduleTaskTool) Description() string {
	return "Create a recurring scheduled task for the current chat. " +
		"The task will automatically run the given prompt or message on the requested schedule. " +
		"Schedules must be at least one minute, for example \"1h\", \"30m\", \"daily@09:00\", or \"weekly:mon@09:00\"."
}

func (scheduleTaskTool) Schema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "prompt": {
      "type": "string",
      "description": "The prompt to run or the message to send each time the task fires."
    },
    "schedule": {
      "type": "string",
      "description": "Recurrence rule, e.g. '1h', '30m', 'daily@09:00', 'weekly:mon@09:00'."
    },
    "execution_mode": {
      "type": "string",
      "enum": ["ai_prompt", "fixed_text"],
      "description": "ai_prompt: run the prompt through the AI each time (default). fixed_text: send the prompt verbatim."
    },
    "title": {
      "type": "string",
      "description": "Optional human-readable title for the task."
    },
    "max_runs": {
      "type": "integer",
      "description": "Maximum number of executions; 0 or omit for unlimited."
    }
  },
  "required": ["prompt", "schedule"]
}`)
}

func (scheduleTaskTool) ReadOnly() bool { return false }

func (t scheduleTaskTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	var a scheduleTaskArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "", err
	}
	a.Prompt = strings.TrimSpace(a.Prompt)
	a.Schedule = strings.TrimSpace(a.Schedule)
	if a.Prompt == "" {
		return "", errors.New("prompt is required")
	}
	if a.Schedule == "" {
		return "", errors.New("schedule is required")
	}

	s, ok := scheduler.FromContext(ctx)
	if !ok || s == nil {
		return "", errors.New("scheduling is not available in this context")
	}

	cc, ok := scheduler.ChatContextFromContext(ctx)
	if !ok {
		return "", errors.New("could not determine the current chat from context")
	}
	if cc.Scheduled {
		return "", errors.New("scheduled runs cannot create or modify scheduled tasks")
	}

	mode := strings.TrimSpace(a.ExecutionMode)
	if mode == "" {
		mode = string(scheduler.ExecutionModeAIPrompt)
	}
	executorKind := scheduler.ExecutorBotAgent
	if mode == string(scheduler.ExecutionModeFixedText) {
		executorKind = scheduler.ExecutorBotMessage
	}

	title := strings.TrimSpace(a.Title)
	if title == "" {
		title = truncateString(a.Prompt, 40)
	}

	task := scheduler.ScheduledTask{
		ID:            generateScheduleTaskID(),
		Title:         title,
		Prompt:        a.Prompt,
		Schedule:      a.Schedule,
		ExecutionMode: mode,
		ExecutorKind:  executorKind,
		Platform:      cc.Platform,
		ConnectionID:  cc.ConnectionID,
		Domain:        cc.Domain,
		ChatType:      cc.ChatType,
		ChatID:        cc.ChatID,
		UserID:        cc.UserID,
		ThreadID:      cc.ThreadID,
		CreatedAt:     time.Now().UnixMilli(),
		MaxRuns:       a.MaxRuns,
		Timezone:      time.Local.String(),
	}
	if err := s.CreateTask(task); err != nil {
		return "", fmt.Errorf("failed to create scheduled task: %w", err)
	}

	return fmt.Sprintf("已创建定时任务 %q（%s），每 %s 执行一次。",
		task.ID, task.Title, task.Schedule), nil
}

func generateScheduleTaskID() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("task-%d", time.Now().UnixNano())
	}
	return "sch_" + hex.EncodeToString(b)
}

func truncateString(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

type scheduleTaskIDArgs struct {
	TaskID       string `json:"task_id"`
	TitleKeyword string `json:"title_keyword,omitempty"`
}

type scheduleListTool struct{}

func (scheduleListTool) Name() string { return "schedule_list" }
func (scheduleListTool) Description() string {
	return "List scheduled tasks for the current chat so the assistant can inspect, confirm, or choose one to modify."
}
func (scheduleListTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}
func (scheduleListTool) ReadOnly() bool { return true }
func (scheduleListTool) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	s, cc, err := scheduleContext(ctx)
	if err != nil {
		return "", err
	}
	tasks := s.ListTasks(scheduler.TaskFilter{
		Platform:     cc.Platform,
		ConnectionID: cc.ConnectionID,
		ChatID:       cc.ChatID,
	})
	if len(tasks) == 0 {
		return "当前聊天没有定时任务。", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "当前聊天共有 %d 个定时任务:\n", len(tasks))
	for _, task := range tasks {
		status := "enabled"
		if !task.Enabled {
			status = "paused"
		}
		next := "unscheduled"
		if task.NextRunAt != 0 {
			next = time.UnixMilli(task.NextRunAt).Format(time.RFC3339)
		}
		last := "never"
		if task.LastRunAt != 0 {
			last = time.UnixMilli(task.LastRunAt).Format(time.RFC3339)
		}
		fmt.Fprintf(&b, "- %s | %s | %s | runs=%d | last=%s | next=%s", task.ID, task.Title, status, task.RunCount, last, next)
		if task.LastError != "" {
			fmt.Fprintf(&b, " | last_error=%s", task.LastError)
		}
		b.WriteByte('\n')
	}
	return strings.TrimSpace(b.String()), nil
}

type scheduleCancelTool struct{}

func (scheduleCancelTool) Name() string { return "schedule_cancel" }
func (scheduleCancelTool) Description() string {
	return "Cancel (delete) a scheduled task created for the current chat. Use schedule_list first when you need the task id."
}
func (scheduleCancelTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"task_id":{"type":"string","description":"The scheduled task id to cancel."},"title_keyword":{"type":"string","description":"Optional keyword to match the task title or prompt when task_id is not provided."}},"required":[]}`)
}
func (scheduleCancelTool) ReadOnly() bool { return false }
func (scheduleCancelTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	return executeScheduleByID(ctx, args, "取消", func(s *scheduler.Scheduler, id string) error {
		return s.DeleteTask(id)
	})
}

type scheduleResumeTool struct{}

func (scheduleResumeTool) Name() string { return "schedule_resume" }
func (scheduleResumeTool) Description() string {
	return "Resume a paused scheduled task in the current chat."
}
func (scheduleResumeTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"task_id":{"type":"string","description":"The scheduled task id to resume."}},"required":["task_id"]}`)
}
func (scheduleResumeTool) ReadOnly() bool { return false }
func (scheduleResumeTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	return executeScheduleByID(ctx, args, "恢复", func(s *scheduler.Scheduler, id string) error {
		return s.ResumeTask(id)
	})
}

type scheduleDeleteTool struct{}

func (scheduleDeleteTool) Name() string { return "schedule_delete" }
func (scheduleDeleteTool) Description() string {
	return "Delete a scheduled task created for the current chat. Use schedule_list first when you need the task id."
}
func (scheduleDeleteTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"task_id":{"type":"string","description":"The scheduled task id to delete."},"title_keyword":{"type":"string","description":"Optional keyword to match the task title or prompt when task_id is not provided."}},"required":[]}`)
}
func (scheduleDeleteTool) ReadOnly() bool { return false }
func (scheduleDeleteTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	return executeScheduleByID(ctx, args, "删除", func(s *scheduler.Scheduler, id string) error {
		return s.DeleteTask(id)
	})
}

type schedulePauseTool struct{}

func (schedulePauseTool) Name() string { return "schedule_pause" }
func (schedulePauseTool) Description() string {
	return "Pause a scheduled task in the current chat. The task can be resumed later with schedule_resume."
}
func (schedulePauseTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"task_id":{"type":"string","description":"The scheduled task id to pause."},"title_keyword":{"type":"string","description":"Optional keyword to match the task title or prompt when task_id is not provided."}},"required":[]}`)
}
func (schedulePauseTool) ReadOnly() bool { return false }
func (schedulePauseTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	return executeScheduleByID(ctx, args, "暂停", func(s *scheduler.Scheduler, id string) error {
		return s.PauseTask(id)
	})
}

func executeScheduleByID(ctx context.Context, args json.RawMessage, action string, fn func(*scheduler.Scheduler, string) error) (string, error) {
	var a scheduleTaskIDArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return "", err
	}
	s, cc, err := scheduleContext(ctx)
	if err != nil {
		return "", err
	}
	if cc.Scheduled {
		return "", errors.New("scheduled runs cannot create or modify scheduled tasks")
	}
	task, err := ownedTaskForCurrentChat(s, cc, a.TaskID, a.TitleKeyword)
	if err != nil {
		return "", err
	}
	if err := fn(s, task.ID); err != nil {
		return "", err
	}
	return fmt.Sprintf("已%s定时任务 %q。", action, task.ID), nil
}

func scheduleContext(ctx context.Context) (*scheduler.Scheduler, scheduler.ChatContext, error) {
	s, ok := scheduler.FromContext(ctx)
	if !ok || s == nil {
		return nil, scheduler.ChatContext{}, errors.New("scheduling is not available in this context")
	}
	cc, ok := scheduler.ChatContextFromContext(ctx)
	if !ok {
		return nil, scheduler.ChatContext{}, errors.New("could not determine the current chat from context")
	}
	return s, cc, nil
}

func ownedTaskForCurrentChat(s *scheduler.Scheduler, cc scheduler.ChatContext, taskID, titleKeyword string) (scheduler.ScheduledTask, error) {
	taskID = strings.TrimSpace(taskID)
	titleKeyword = strings.TrimSpace(titleKeyword)
	if taskID == "" && titleKeyword == "" {
		return scheduler.ScheduledTask{}, errors.New("task_id or title_keyword is required")
	}

	filter := scheduler.TaskFilter{
		Platform:     cc.Platform,
		ConnectionID: cc.ConnectionID,
		ChatID:       cc.ChatID,
	}
	if taskID != "" {
		task, ok := s.GetTask(taskID)
		if !ok {
			return scheduler.ScheduledTask{}, fmt.Errorf("scheduled task %q not found", taskID)
		}
		if task.Platform != cc.Platform || task.ConnectionID != cc.ConnectionID || task.ChatID != cc.ChatID {
			return scheduler.ScheduledTask{}, fmt.Errorf("scheduled task %q does not belong to the current chat", taskID)
		}
		return checkTaskOwnership(task, cc)
	}

	tasks := s.ListTasks(filter)
	lowerKw := strings.ToLower(titleKeyword)
	for _, task := range tasks {
		if strings.Contains(strings.ToLower(task.Title), lowerKw) || strings.Contains(strings.ToLower(task.Prompt), lowerKw) {
			return checkTaskOwnership(task, cc)
		}
	}
	return scheduler.ScheduledTask{}, fmt.Errorf("no scheduled task matching %q in current chat", titleKeyword)
}

func checkTaskOwnership(task scheduler.ScheduledTask, cc scheduler.ChatContext) (scheduler.ScheduledTask, error) {
	if strings.TrimSpace(task.UserID) != "" && strings.TrimSpace(task.UserID) != strings.TrimSpace(cc.UserID) {
		return scheduler.ScheduledTask{}, fmt.Errorf("scheduled task %q belongs to another user", task.ID)
	}
	return task, nil
}
