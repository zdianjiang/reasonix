package bot

import (
	"context"
	"fmt"
	"strings"
	"time"

	scheduler "reasonix/internal/schedule"
)

// SendText implements scheduler.Gateway by sending a plain text message to the
// chat recorded in the scheduled task.
func (gw *BotGateway) SendText(ctx context.Context, task scheduler.ScheduledTask, text string) error {
	_, err := gw.SendTextToAdapter(ctx,
		strings.TrimSpace(task.ConnectionID),
		strings.TrimSpace(task.Domain),
		strings.TrimSpace(task.ChatID),
		ChatType(strings.TrimSpace(task.ChatType)),
		text,
	)
	if err != nil {
		return fmt.Errorf("scheduled send text: %w", err)
	}
	return nil
}

// RunPrompt implements scheduler.Gateway by running a synthetic inbound
// message through the normal turn pipeline synchronously.
func (gw *BotGateway) RunPrompt(ctx context.Context, task scheduler.ScheduledTask, prompt string) error {
	binding, ok := gw.adapterForTask(task)
	if !ok {
		return fmt.Errorf("no adapter for connection %q (domain %q)", task.ConnectionID, task.Domain)
	}
	msg := InboundMessage{
		Platform:     binding.Platform,
		ConnectionID: binding.ID,
		Domain:       binding.Domain,
		ChatType:     ChatType(strings.TrimSpace(task.ChatType)),
		ChatID:       strings.TrimSpace(task.ChatID),
		UserID:       strings.TrimSpace(task.UserID),
		ThreadID:     strings.TrimSpace(task.ThreadID),
		Text:         prompt,
		MessageID:    fmt.Sprintf("scheduled:%s:%d", task.ID, time.Now().UnixNano()),
		SessionID:    "task:" + task.ID,
	}
	return gw.runScheduledPrompt(ctx, binding, msg)
}

// adapterForTask finds the adapter binding matching a scheduled task's platform
// and connection.
func (gw *BotGateway) adapterForTask(task scheduler.ScheduledTask) (AdapterBinding, bool) {
	connID := strings.TrimSpace(task.ConnectionID)
	domain := strings.TrimSpace(task.Domain)
	platform := Platform(strings.TrimSpace(task.Platform))

	gw.mu.Lock()
	defer gw.mu.Unlock()
	for _, binding := range gw.adapters {
		if binding.Platform != platform {
			continue
		}
		if connID != "" && strings.TrimSpace(binding.ID) != connID {
			continue
		}
		if domain != "" && !strings.EqualFold(strings.TrimSpace(binding.Domain), domain) {
			continue
		}
		return binding, true
	}
	return AdapterBinding{}, false
}

// handleScheduleCommand routes /schedule list/cancel/pause/resume.
func (gw *BotGateway) handleScheduleCommand(ctx context.Context, adapter Adapter, msg InboundMessage) {
	parts := strings.Fields(strings.TrimSpace(msg.Text))
	if len(parts) < 2 {
		_ = gw.sendText(ctx, adapter, msg, "用法: /schedule list | cancel <id> | pause <id> | resume <id>")
		return
	}
	sub := strings.ToLower(parts[1])
	switch sub {
	case "list":
		_ = gw.sendText(ctx, adapter, msg, gw.scheduleListText(msg))
	case "cancel", "delete", "pause", "resume":
		if len(parts) < 3 {
			_ = gw.sendText(ctx, adapter, msg, fmt.Sprintf("用法: /schedule %s <id>", sub))
			return
		}
		id := parts[2]
		task, ok := gw.scheduler.GetTask(id)
		if !ok {
			_ = gw.sendText(ctx, adapter, msg, fmt.Sprintf("未找到任务 %s", id))
			return
		}
		if !scheduledTaskInChat(task, msg) {
			_ = gw.sendText(ctx, adapter, msg, fmt.Sprintf("未找到当前聊天中的任务 %s", id))
			return
		}
		if !task.IsOwner(msg.UserID) && !gw.checkCommandRole(msg.Platform, msg, "admin") {
			_ = gw.sendText(ctx, adapter, msg, "抱歉，你没有执行此 bot 命令的权限。")
			return
		}
		var err error
		switch sub {
		case "cancel", "delete":
			err = gw.scheduler.DeleteTask(id)
		case "pause":
			err = gw.scheduler.PauseTask(id)
		case "resume":
			err = gw.scheduler.ResumeTask(id)
		}
		if err != nil {
			_ = gw.sendText(ctx, adapter, msg, fmt.Sprintf("操作失败: %v", err))
			return
		}
		_ = gw.sendText(ctx, adapter, msg, fmt.Sprintf("已%s任务 %s", scheduleActionLabel(sub), id))
	default:
		_ = gw.sendText(ctx, adapter, msg, "用法: /schedule list | cancel <id> | pause <id> | resume <id>")
	}
}

func scheduledTaskInChat(task scheduler.ScheduledTask, msg InboundMessage) bool {
	return task.Platform == string(msg.Platform) &&
		task.ConnectionID == msg.ConnectionID &&
		task.ChatID == msg.ChatID
}

func (gw *BotGateway) scheduleListText(msg InboundMessage) string {
	filter := scheduler.TaskFilter{
		Platform:     string(msg.Platform),
		ConnectionID: msg.ConnectionID,
		ChatID:       msg.ChatID,
	}
	tasks := gw.scheduler.ListTasks(filter)
	if len(tasks) == 0 {
		return "当前聊天没有定时任务。"
	}
	var lines []string
	lines = append(lines, fmt.Sprintf("当前聊天共有 %d 个定时任务:", len(tasks)))
	for _, t := range tasks {
		status := "启用"
		if !t.Enabled {
			status = "暂停"
		}
		next := "未安排"
		if t.NextRunAt != 0 {
			next = time.UnixMilli(t.NextRunAt).Format("01-02 15:04")
		}
		last := "从未执行"
		if t.LastRunAt != 0 {
			last = time.UnixMilli(t.LastRunAt).Format("01-02 15:04")
		}
		line := fmt.Sprintf("- %s: %s (%s) [%s] 已执行: %d 次，最近: %s，下次: %s", t.ID, t.Title, t.Schedule, status, t.RunCount, last, next)
		if t.LastError != "" {
			line += " 最近失败: " + t.LastError
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func scheduleActionLabel(action string) string {
	switch action {
	case "cancel", "delete":
		return "删除"
	case "pause":
		return "暂停"
	case "resume":
		return "恢复"
	default:
		return action
	}
}

func (gw *BotGateway) runScheduledPrompt(ctx context.Context, binding AdapterBinding, msg InboundMessage) error {
	msg.Platform = binding.Platform
	if msg.ConnectionID == "" {
		msg.ConnectionID = binding.ID
	}
	if msg.Domain == "" {
		msg.Domain = binding.Domain
	}
	key := BuildSessionKey(msg.Session())
	result := gw.sessions.TryAcquireWithQueue(key, msg, QueueOptions{
		Mode: QueueModeFollowup,
		Cap:  1,
		Drop: QueueDropNew,
	})
	if result.Rejected || result.Queued || !result.Acquired {
		return fmt.Errorf("scheduled task could not start because the session is busy")
	}
	return gw.runTurnWithLifecycle(ctx, binding.Adapter, key, msg, nil)
}
