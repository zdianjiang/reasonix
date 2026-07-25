package bot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"reasonix/internal/event"
)

type failingAttachmentAdapter struct {
	*fakeAdapter
}

func (f *failingAttachmentAdapter) Send(ctx context.Context, msg OutboundMessage) (SendResult, error) {
	if msg.Attachment != nil {
		return SendResult{}, fmt.Errorf("attachment transport failed")
	}
	return f.fakeAdapter.Send(ctx, msg)
}

func TestApprovalCardCarriesChatType(t *testing.T) {
	card := approvalCard(event.Approval{ID: "approval-1"}, ChatDM, "allowed-user")
	if len(card.Elements) < 2 {
		t.Fatalf("approval card elements = %d, want at least 2", len(card.Elements))
	}
	actions, ok := card.Elements[1].Extra["actions"].([]map[string]any)
	if !ok || len(actions) == 0 {
		t.Fatalf("approval card actions missing or wrong type: %#v", card.Elements[1].Extra["actions"])
	}
	value, ok := actions[0]["value"].(map[string]string)
	if !ok {
		t.Fatalf("approval action value has wrong type: %#v", actions[0]["value"])
	}
	if value["command"] != "/approve approval-1" {
		t.Fatalf("command = %q, want /approve approval-1", value["command"])
	}
	if value["chat_type"] != string(ChatDM) {
		t.Fatalf("chat_type = %q, want %q", value["chat_type"], ChatDM)
	}
	if value["user_id"] != "allowed-user" {
		t.Fatalf("user_id = %q, want allowed-user", value["user_id"])
	}
}

func TestApprovalCardActionsAreToolAgnostic(t *testing.T) {
	for _, approval := range []event.Approval{
		{ID: "plan-1", Tool: "exit_plan_mode", Subject: "plan"},
		{ID: "task-1", Tool: "task", Subject: "run subtask"},
	} {
		card := approvalCard(approval, ChatGroup, "allowed-user")
		if len(card.Elements) < 2 {
			t.Fatalf("%s card elements = %d, want actions", approval.Tool, len(card.Elements))
		}
		actions, ok := card.Elements[1].Extra["actions"].([]map[string]any)
		if !ok || len(actions) != 2 {
			t.Fatalf("%s actions missing or wrong type: %#v", approval.Tool, card.Elements[1].Extra["actions"])
		}
		allow, ok := actions[0]["value"].(map[string]string)
		if !ok {
			t.Fatalf("%s allow value has wrong type: %#v", approval.Tool, actions[0]["value"])
		}
		deny, ok := actions[1]["value"].(map[string]string)
		if !ok {
			t.Fatalf("%s deny value has wrong type: %#v", approval.Tool, actions[1]["value"])
		}
		if allow["command"] != "/approve "+approval.ID || deny["command"] != "/deny "+approval.ID {
			t.Fatalf("%s commands = %q/%q, want approve/deny by id", approval.Tool, allow["command"], deny["command"])
		}
	}
}

func TestAskCardAddsAnswerButtonsForSingleChoice(t *testing.T) {
	card := askCard(event.Ask{
		ID: "ask-1",
		Questions: []event.AskQuestion{{
			ID:     "q1",
			Prompt: "Choose one",
			Options: []event.AskOption{
				{Label: "允许一次"},
				{Label: "拒绝"},
			},
		}},
	}, "fallback", ChatDM, "allowed-user")

	if len(card.Elements) != 2 {
		t.Fatalf("ask card elements = %d, want markdown + actions", len(card.Elements))
	}
	actions, ok := card.Elements[1].Extra["actions"].([]map[string]any)
	if !ok || len(actions) != 2 {
		t.Fatalf("ask card actions missing or wrong type: %#v", card.Elements[1].Extra["actions"])
	}
	value, ok := actions[0]["value"].(map[string]string)
	if !ok {
		t.Fatalf("ask action value has wrong type: %#v", actions[0]["value"])
	}
	if value["command"] != "/answer ask-1 1" {
		t.Fatalf("command = %q, want /answer ask-1 1", value["command"])
	}
	if value["chat_type"] != string(ChatDM) {
		t.Fatalf("chat_type = %q, want %q", value["chat_type"], ChatDM)
	}
	if value["user_id"] != "allowed-user" {
		t.Fatalf("user_id = %q, want allowed-user", value["user_id"])
	}
}

func TestRenderSinkDoesNotFlushMidSentenceOnTimer(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(context.Background(), adapter, "weixin-weixin", "weixin", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)
	sink.lastFlush = time.Now().Add(-2 * time.Second)

	sink.Emit(event.Event{Kind: event.Text, Text: "我是 **"})
	sink.Emit(event.Event{Kind: event.Text, Text: "Reasonix**，一个专注于执行代码任务的 AI 编程助手"})

	if sent := adapter.sentMessages(); len(sent) != 0 {
		t.Fatalf("sent = %+v, want no mid-sentence flush", sent)
	}

	sink.Emit(event.Event{Kind: event.TurnDone})
	sent := adapter.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("sent count = %d, want final flush only", len(sent))
	}
	if sent[0].Text != "我是 **Reasonix**，一个专注于执行代码任务的 AI 编程助手" {
		t.Fatalf("sent text = %q, want combined sentence", sent[0].Text)
	}
}

func TestRenderSinkKeepsSemanticTextUntilFinalResult(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(context.Background(), adapter, "weixin-weixin", "weixin", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)
	sink.lastFlush = time.Now().Add(-2 * time.Second)

	sink.Emit(event.Event{Kind: event.Text, Text: "第一句。"})

	if sent := adapter.sentMessages(); len(sent) != 0 {
		t.Fatalf("sent = %+v, want semantic text held until final result", sent)
	}

	sink.Emit(event.Event{Kind: event.TurnDone})
	sent := adapter.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("sent count = %d, want final result only", len(sent))
	}
	if sent[0].Text != "第一句。" {
		t.Fatalf("sent text = %q, want final result", sent[0].Text)
	}
}

func TestRenderSinkFinalFlushKeepsTrailingEmojiWithReply(t *testing.T) {
	adapter := newFakeAdapter(PlatformFeishu, "fake-feishu")
	sink := newRenderSink(context.Background(), adapter, "feishu-feishu", "feishu", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)
	sink.Emit(event.Event{Kind: event.Text, Text: "任务已删除。"})
	sink.Emit(event.Event{Kind: event.Text, Text: "✅"})
	sink.Emit(event.Event{Kind: event.TurnDone})

	sent := adapter.sentMessages()
	if len(sent) != 1 || sent[0].Text != "任务已删除。✅" {
		t.Fatalf("sent = %+v, want one complete reply", sent)
	}
}

func TestRenderSinkFinalFlushKeepsChunkLimit(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(context.Background(), adapter, "weixin-weixin", "weixin", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)
	sink.buf.WriteString(strings.Repeat("长", renderMaxChunkRunes*2+10))

	sink.Emit(event.Event{Kind: event.TurnDone})

	sent := adapter.sentMessages()
	if len(sent) < 2 {
		t.Fatalf("sent count = %d, want chunked final flush", len(sent))
	}
	for i, msg := range sent {
		if got := len([]rune(msg.Text)); got > renderMaxChunkRunes {
			t.Fatalf("sent[%d] runes = %d, want <= %d", i, got, renderMaxChunkRunes)
		}
	}
}

func TestRenderSinkConsumesEmptyWhitespacePrefix(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(context.Background(), adapter, "weixin-weixin", "weixin", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)
	sink.buf.WriteString("\n工具状态")

	sink.flushPrefix(1)

	if got := sink.buf.String(); got != "工具状态" {
		t.Fatalf("buffer = %q, want leading newline consumed", got)
	}
	if sent := adapter.sentMessages(); len(sent) != 0 {
		t.Fatalf("sent = %+v, want no empty outbound message", sent)
	}
}

func TestRenderSinkSendsProgressWithoutToolOutput(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(context.Background(), adapter, "weixin-weixin", "weixin", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{ToolDispatch: renderRouteIM}, nil, nil)

	sink.Emit(event.Event{Kind: event.TurnStarted})
	sink.Emit(event.Event{Kind: event.ToolDispatch, Tool: event.Tool{ID: "tool-1", Name: "read_file", ReadOnly: true}})
	sink.Emit(event.Event{Kind: event.ToolResult, Tool: event.Tool{ID: "tool-1", Name: "read_file", Output: "secret output that should stay out of IM"}})
	sink.Emit(event.Event{Kind: event.Text, Text: "完成。"})
	sink.Emit(event.Event{Kind: event.TurnDone})

	sent := adapter.sentMessages()
	if len(sent) != 2 {
		t.Fatalf("sent count = %d, want one progress message plus final result: %+v", len(sent), sent)
	}
	if sent[0].Text != "正在执行: read_file" {
		t.Fatalf("progress text = %q, want concise tool status", sent[0].Text)
	}
	if strings.Contains(sent[0].Text, "secret output") || strings.Contains(sent[1].Text, "secret output") {
		t.Fatalf("tool output leaked into IM messages: %+v", sent)
	}
	if sent[1].Text != "完成。" {
		t.Fatalf("final text = %q, want final result only", sent[1].Text)
	}
}

func TestRenderSinkLimitsProgressMessages(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(context.Background(), adapter, "weixin-weixin", "weixin", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{ToolDispatch: renderRouteIM}, nil, nil)

	for i := 0; i < renderMaxProgressMessages+2; i++ {
		sink.lastProgress = time.Now().Add(-renderProgressMinInterval)
		sink.Emit(event.Event{Kind: event.ToolDispatch, Tool: event.Tool{ID: "tool", Name: "bash"}})
	}

	sent := adapter.sentMessages()
	if len(sent) != renderMaxProgressMessages {
		t.Fatalf("sent count = %d, want capped progress count %d", len(sent), renderMaxProgressMessages)
	}
}

func TestRenderSinkSuppressesReasoning(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(context.Background(), adapter, "weixin-weixin", "weixin", "chat-1", ChatDM, "user-1", "", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)

	sink.Emit(event.Event{Kind: event.Reasoning, Text: "internal reasoning"})
	sink.Emit(event.Event{Kind: event.Text, Text: "可见结果"})
	sink.Emit(event.Event{Kind: event.TurnDone})

	sent := adapter.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("sent count = %d, want one final result", len(sent))
	}
	if strings.Contains(sent[0].Text, "internal reasoning") {
		t.Fatalf("reasoning leaked into IM message: %q", sent[0].Text)
	}
}

func TestRenderSinkPreservesTextAttachmentOrder(t *testing.T) {
	adapter := newFakeAdapter(PlatformFeishu, "fake-feishu")
	sink := newRenderSink(context.Background(), adapter, "feishu-feishu", "feishu", "chat-1", ChatDM, "user-1", "/workspace", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)

	sink.Emit(event.Event{Kind: event.Text, Text: "先看图片"})
	sink.Emit(event.Event{Kind: event.ReplyAttachmentEvent, Attachment: &event.ReplyAttachment{
		Kind: "image",
		Path: "artifacts/preview.png",
		Name: "preview.png",
	}})
	sink.Emit(event.Event{Kind: event.Text, Text: "再看文件"})
	sink.Emit(event.Event{Kind: event.ReplyAttachmentEvent, Attachment: &event.ReplyAttachment{
		Kind: "file",
		Path: "artifacts/report.pdf",
		Name: "report.pdf",
	}})
	sink.Emit(event.Event{Kind: event.TurnDone})

	sent := adapter.sentMessages()
	if len(sent) != 4 {
		t.Fatalf("sent count = %d, want text,image,text,file", len(sent))
	}
	if sent[0].Text != "先看图片" {
		t.Fatalf("sent[0].text = %q, want first text", sent[0].Text)
	}
	if sent[1].Attachment == nil || sent[1].Attachment.Kind != "image" || sent[1].Attachment.Path != "artifacts/preview.png" {
		t.Fatalf("sent[1] = %+v, want image attachment", sent[1])
	}
	if sent[2].Text != "再看文件" {
		t.Fatalf("sent[2].text = %q, want second text", sent[2].Text)
	}
	if sent[3].Attachment == nil || sent[3].Attachment.Kind != "file" || sent[3].Attachment.Path != "artifacts/report.pdf" {
		t.Fatalf("sent[3] = %+v, want file attachment", sent[3])
	}
}

func TestRenderSinkReportsAttachmentSendFailure(t *testing.T) {
	adapter := &failingAttachmentAdapter{fakeAdapter: newFakeAdapter(PlatformFeishu, "fake-feishu")}
	sink := newRenderSink(context.Background(), adapter, "feishu-feishu", "feishu", "chat-1", ChatDM, "user-1", "/workspace", "msg-1", slog.New(slog.NewTextHandler(io.Discard, nil)), renderObservability{}, nil, nil)

	sink.Emit(event.Event{Kind: event.ReplyAttachmentEvent, Attachment: &event.ReplyAttachment{
		Kind: "file",
		Path: "artifacts/report.pdf",
		Name: "report.pdf",
	}})

	sent := adapter.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("sent count = %d, want one warning message", len(sent))
	}
	if !strings.Contains(sent[0].Text, "附件发送失败") {
		t.Fatalf("warning text = %q, want attachment failure notice", sent[0].Text)
	}
}

func TestRenderSinkLogsReasoningOnlyOnce(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	sink := newRenderSink(
		context.Background(),
		adapter,
		"weixin-weixin",
		"weixin",
		"chat-1",
		ChatDM,
		"user-1",
		"",
		"msg-1",
		logger,
		renderObservability{Reasoning: renderRouteLog},
		nil,
		nil,
	)

	sink.Emit(event.Event{Kind: event.Reasoning, Text: "internal reasoning"})
	sink.Emit(event.Event{Kind: event.Text, Text: "answer part 1"})
	sink.Emit(event.Event{Kind: event.Text, Text: "answer part 2"})
	sink.Emit(event.Event{Kind: event.TurnDone})

	logs := buf.String()
	if got := strings.Count(logs, "bot reasoning"); got != 1 {
		t.Fatalf("reasoning log count = %d, want 1\nlogs:\n%s", got, logs)
	}
}

func TestRenderSinkLogsReasoningAndToolProgress(t *testing.T) {
	adapter := newFakeAdapter(PlatformWeixin, "fake-weixin")
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	sink := newRenderSink(
		context.Background(),
		adapter,
		"weixin-weixin",
		"weixin",
		"chat-1",
		ChatDM,
		"user-1",
		"",
		"msg-1",
		logger,
		renderObservability{
			Reasoning:    renderRouteLog,
			ToolDispatch: renderRouteLog,
			ToolProgress: renderRouteLog,
			ToolResult:   renderRouteLog,
		},
		nil,
		nil,
	)

	sink.Emit(event.Event{Kind: event.Reasoning, Text: "internal reasoning"})
	sink.Emit(event.Event{Kind: event.Text, Text: "final answer"})
	sink.Emit(event.Event{Kind: event.ToolDispatch, Tool: event.Tool{ID: "tool-1", Name: "bash"}})
	sink.Emit(event.Event{Kind: event.ToolProgress, Tool: event.Tool{ID: "tool-1", Name: "bash", Output: "line 1"}})
	sink.Emit(event.Event{Kind: event.ToolResult, Tool: event.Tool{ID: "tool-1", Name: "bash", Output: "done"}})

	logs := buf.String()
	for _, want := range []string{"bot reasoning", "bot tool dispatch", "bot tool progress", "bot tool result"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q:\n%s", want, logs)
		}
	}
	if got := adapter.sentMessages(); len(got) != 0 {
		t.Fatalf("IM messages = %+v, want none when only log routes are enabled", got)
	}
}
