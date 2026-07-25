package bot

import (
	"context"
	"testing"
	"time"

	scheduler "reasonix/internal/schedule"
)

func TestGatewaySendTextScheduledTask(t *testing.T) {
	adapter := newFakeAdapter(PlatformFeishu, "feishu")
	gw := NewGateway(GatewayConfig{
		Enabled: map[Platform]bool{PlatformFeishu: true},
	}, map[Platform]Adapter{PlatformFeishu: adapter}, nil)

	task := scheduler.ScheduledTask{
		ID:           "test-text",
		Prompt:       "Hello from scheduler",
		Platform:     string(PlatformFeishu),
		ConnectionID: "feishu",
		ChatType:     string(ChatDM),
		ChatID:       "chat-1",
	}
	if err := gw.SendText(context.Background(), task, task.Prompt); err != nil {
		t.Fatalf("SendText: %v", err)
	}

	sent := adapter.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(sent))
	}
	if sent[0].ChatID != "chat-1" {
		t.Errorf("chat_id = %q, want chat-1", sent[0].ChatID)
	}
	if sent[0].Text != "Hello from scheduler" {
		t.Errorf("text = %q, want %q", sent[0].Text, "Hello from scheduler")
	}
}

func TestGatewayRunPromptQueuesMessage(t *testing.T) {
	adapter := newFakeAdapter(PlatformFeishu, "feishu")
	gw := NewGateway(GatewayConfig{
		Enabled: map[Platform]bool{PlatformFeishu: true},
		Allowlist: AllowlistConfig{
			AllowAll: true,
		},
	}, map[Platform]Adapter{PlatformFeishu: adapter}, nil)

	task := scheduler.ScheduledTask{
		ID:           "test-prompt",
		Prompt:       "Tell me a joke",
		Platform:     string(PlatformFeishu),
		ConnectionID: "feishu",
		ChatType:     string(ChatDM),
		ChatID:       "chat-2",
		UserID:       "user-2",
	}
	err := gw.RunPrompt(context.Background(), task, task.Prompt)

	// RunPrompt is synchronous. In tests there is no real model API key, so the
	// controller build may fail; the invariant we check here is that the gateway
	// either ran the turn successfully or at least sent an error reply back to
	// the chat, without hanging.
	if err != nil && len(adapter.sentMessages()) == 0 {
		t.Fatalf("RunPrompt failed (%v) and no message was sent", err)
	}
	if len(adapter.sentMessages()) == 0 {
		t.Fatal("expected adapter.Send to be called for the synthetic turn")
	}
}

func TestGatewaySchedulerStartedAndStopped(t *testing.T) {
	adapter := newFakeAdapter(PlatformFeishu, "feishu")
	gw := NewGateway(GatewayConfig{
		Enabled: map[Platform]bool{PlatformFeishu: true},
	}, map[Platform]Adapter{PlatformFeishu: adapter}, nil)

	if gw.scheduler == nil {
		t.Fatal("expected scheduler to be created")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := gw.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !gw.scheduler.Running() {
		t.Fatal("expected scheduler to be running after Start")
	}

	gw.Stop()
	// Scheduler stops asynchronously; give it a moment.
	time.Sleep(50 * time.Millisecond)
	if gw.scheduler.Running() {
		t.Fatal("expected scheduler to be stopped after Stop")
	}
}
