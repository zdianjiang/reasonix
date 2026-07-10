package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"reasonix/internal/event"
	"reasonix/internal/tool"
)

func init() {
	tool.RegisterBuiltin(sendAttachmentTool{kind: "file"})
	tool.RegisterBuiltin(sendAttachmentTool{kind: "image"})
}

type sendAttachmentTool struct {
	kind    string
	workDir string
}

func (t sendAttachmentTool) Name() string {
	if t.kind == "image" {
		return "message_send_image"
	}
	return "message_send_file"
}

func (t sendAttachmentTool) Description() string {
	if t.kind == "image" {
		return "Send a workspace image to the current bot chat message stream. Use when the user asks you to send or upload an image file."
	}
	return "Send a workspace file to the current bot chat message stream. Use when the user asks you to send or upload a file."
}

func (t sendAttachmentTool) Schema() json.RawMessage {
	desc := "Workspace-relative file path to send."
	if t.kind == "image" {
		desc = "Workspace-relative image path to send."
	}
	return json.RawMessage(fmt.Sprintf(`{"type":"object","properties":{"path":{"type":"string","description":%q}},"required":["path"]}`, desc))
}

// ReadOnly is true: sending a reply attachment does not mutate the local host.
func (t sendAttachmentTool) ReadOnly() bool { return true }

func (t sendAttachmentTool) Execute(ctx context.Context, args json.RawMessage) (string, error) {
	var p struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(args, &p); err != nil {
		return "", fmt.Errorf("invalid args: %w", err)
	}
	path := strings.TrimSpace(p.Path)
	if path == "" {
		return "", fmt.Errorf("path is required")
	}
	abs := resolveIn(t.workDir, path)
	rel, name, contentType, err := validateSendAttachment(abs, t.workDir, t.kind)
	if err != nil {
		return "", err
	}
	if sink, ok := tool.EventSinkFromContext(ctx); ok {
		sink.Emit(event.Event{
			Kind: event.ReplyAttachmentEvent,
			Attachment: &event.ReplyAttachment{
				Kind:        t.kind,
				Path:        rel,
				Name:        name,
				ContentType: contentType,
			},
		})
	}
	return fmt.Sprintf("queued %s attachment %s", t.kind, rel), nil
}

func validateSendAttachment(absPath, workDir, kind string) (rel string, name string, contentType string, err error) {
	if strings.TrimSpace(absPath) == "" {
		return "", "", "", fmt.Errorf("path is required")
	}
	if strings.TrimSpace(workDir) == "" {
		return "", "", "", fmt.Errorf("%s is only available when a workspace root is configured", kind)
	}
	root, err := filepath.Abs(workDir)
	if err != nil {
		return "", "", "", err
	}
	target, err := filepath.Abs(absPath)
	if err != nil {
		return "", "", "", err
	}
	rel, err = filepath.Rel(root, target)
	if err != nil || !filepath.IsLocal(rel) {
		return "", "", "", fmt.Errorf("%s path %q is outside the workspace", kind, absPath)
	}
	info, err := os.Stat(target)
	if err != nil {
		return "", "", "", err
	}
	if info.IsDir() {
		return "", "", "", fmt.Errorf("%s path %q is a directory", kind, absPath)
	}
	name = filepath.Base(target)
	contentType = mime.TypeByExtension(strings.ToLower(filepath.Ext(name)))
	if contentType == "" {
		f, ferr := os.Open(target)
		if ferr == nil {
			defer f.Close()
			var buf [512]byte
			n, _ := f.Read(buf[:])
			contentType = http.DetectContentType(buf[:n])
		}
	}
	if kind == "image" && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "image/") {
		return "", "", "", fmt.Errorf("message_send_image requires an image file, got %q", name)
	}
	return filepath.ToSlash(rel), name, contentType, nil
}
