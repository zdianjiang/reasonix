package builtin

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"reasonix/internal/event"
	"reasonix/internal/tool"
)

type captureEventSink struct {
	events []event.Event
}

func (s *captureEventSink) Emit(e event.Event) { s.events = append(s.events, e) }

func TestMessageSendFileEmitsReplyAttachmentEvent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "artifacts", "report.txt")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl := byName(Workspace{Dir: dir}.Tools())["message_send_file"]
	sink := &captureEventSink{}
	ctx := tool.WithEventSink(context.Background(), sink)

	out, err := tl.Execute(ctx, argsJSON(t, map[string]any{"path": "artifacts/report.txt"}))
	if err != nil {
		t.Fatalf("message_send_file: %v", err)
	}
	if out == "" {
		t.Fatal("message_send_file returned empty result")
	}
	if len(sink.events) != 1 {
		t.Fatalf("events = %d, want 1", len(sink.events))
	}
	ev := sink.events[0]
	if ev.Kind != event.ReplyAttachmentEvent || ev.Attachment == nil {
		t.Fatalf("event = %+v, want reply attachment", ev)
	}
	if ev.Attachment.Kind != "file" || ev.Attachment.Path != "artifacts/report.txt" {
		t.Fatalf("attachment = %+v, want relative file path", ev.Attachment)
	}
}

func TestMessageSendImageRejectsNonImageFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "artifacts", "report.txt")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl := byName(Workspace{Dir: dir}.Tools())["message_send_image"]
	_, err := tl.Execute(context.Background(), argsJSON(t, map[string]any{"path": "artifacts/report.txt"}))
	if err == nil {
		t.Fatal("message_send_image should reject non-image file")
	}
}

func TestMessageSendFileRejectsOutsideWorkspace(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl := byName(Workspace{Dir: dir}.Tools())["message_send_file"]
	_, err := tl.Execute(context.Background(), argsJSON(t, map[string]any{"path": outside}))
	if err == nil {
		t.Fatal("message_send_file should reject outside workspace")
	}
}
