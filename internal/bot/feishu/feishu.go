// Package feishu 实现飞书自建应用 Bot 适配器。
// 参考 Hermes Agent 的 feishu adapter：
// - 长连接 WebSocket（默认）或 Webhook 模式
// - @mention gating
// - open_id / user_id / union_id 映射
// - 消息去重
// - interactive card 审批/问答
package feishu

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"reasonix/internal/bot"
	"reasonix/internal/config"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

// textContent 飞书消息文本内容结构。
type textContent struct {
	Text string `json:"text"`
}

type postContent struct {
	ZhCN *postBody `json:"zh_cn"`
	EnUS *postBody `json:"en_us"`
}

type postBody struct {
	Title   string          `json:"title"`
	Content [][]postElement `json:"content"`
}

type postElement struct {
	Tag      string `json:"tag"`
	Text     string `json:"text"`
	UserName string `json:"user_name"`
}

type imageContent struct {
	ImageKey string `json:"image_key"`
}

type fileContent struct {
	FileKey  string `json:"file_key"`
	FileName string `json:"file_name"`
}

type decodedIncomingContent struct {
	Text         string
	MentionCount int
	Media        []bot.InboundMedia
}

const feishuPendingReactionEmoji = "OnIt"
const feishuMaxInboundResourceBytes = 25 * 1024 * 1024

// feishuEvent 飞书事件结构。
type feishuEvent struct {
	Schema string          `json:"schema"`
	Header feishuHeader    `json:"header"`
	Event  json.RawMessage `json:"event"`
}

type feishuHeader struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	Token      string `json:"token"`
	CreateTime string `json:"create_time"`
}

type feishuMsgEvent struct {
	MessageID string          `json:"message_id"`
	RootID    string          `json:"root_id"`
	ParentID  string          `json:"parent_id"`
	ChatID    string          `json:"chat_id"`
	ChatType  string          `json:"chat_type"`
	MsgType   string          `json:"msg_type"`
	Content   string          `json:"content"`
	Sender    feishuSender    `json:"sender"`
	Mentions  []feishuMention `json:"mentions"`
}

type feishuSender struct {
	SenderID struct {
		UserID  string `json:"user_id"`
		OpenID  string `json:"open_id"`
		UnionID string `json:"union_id"`
	} `json:"sender_id"`
}

type feishuMention struct {
	Key string `json:"key"`
	ID  struct {
		OpenID string `json:"open_id"`
	} `json:"id"`
}

// adapter 飞书适配器实现。
type adapter struct {
	cfg         config.FeishuBotConfig
	logger      *slog.Logger
	msgCh       chan bot.InboundMessage
	cancel      context.CancelFunc
	client      *lark.Client
	wsClient    *larkws.Client
	download    func(context.Context, string, string, string) (bot.InboundMedia, error)
	sendContent func(context.Context, bot.OutboundMessage, string, string) (bot.SendResult, error)
	uploadImage func(context.Context, []byte) (string, error)
	uploadFile  func(context.Context, string, []byte) (string, error)

	seenMu sync.Mutex
	seen   map[string]bool // 消息去重
}

// New 创建飞书 Bot 适配器。
func New(cfg config.FeishuBotConfig, logger *slog.Logger) bot.Adapter {
	return &adapter{
		cfg:    cfg,
		logger: logger.With("platform", "feishu"),
		seen:   make(map[string]bool),
	}
}

func (a *adapter) Platform() bot.Platform { return bot.PlatformFeishu }
func (a *adapter) Name() string           { return "feishu" }

func (a *adapter) Start(ctx context.Context) error {
	a.msgCh = make(chan bot.InboundMessage, 64)
	ctx, a.cancel = context.WithCancel(ctx)

	mode := a.cfg.Mode
	if mode == "" {
		mode = "webhook"
	}

	switch mode {
	case "webhook":
		// Webhook mode exposes a public HTTP endpoint; without a verification
		// token verificationTokenValid accepts every caller, so fail closed
		// rather than let anyone drive the agent.
		if strings.TrimSpace(a.cfg.VerificationToken) == "" {
			return fmt.Errorf("feishu: webhook mode needs verification_token set — refusing to expose an unauthenticated event endpoint")
		}
		go a.runWebhook(ctx)
	default:
		if _, err := a.appSecret(); err != nil {
			return err
		}
		go a.runWebSocket(ctx)
	}
	return nil
}

func (a *adapter) Stop() error {
	if a.cancel != nil {
		a.cancel()
	}
	if a.wsClient != nil {
		a.wsClient.Close()
	}
	return nil
}

func (a *adapter) Send(ctx context.Context, msg bot.OutboundMessage) (bot.SendResult, error) {
	return a.sendMessage(ctx, msg)
}

func (a *adapter) SendTyping(ctx context.Context, chatID string) error {
	return nil
}

func (a *adapter) Messages() <-chan bot.InboundMessage {
	return a.msgCh
}

func (a *adapter) appSecret() (string, error) {
	secret := os.Getenv(a.cfg.AppSecretEnv)
	if a.cfg.AppID == "" || secret == "" {
		return "", fmt.Errorf("feishu app_id or %s is not configured", a.cfg.AppSecretEnv)
	}
	return secret, nil
}

// runWebSocket 启动飞书 WebSocket 长连接。
func (a *adapter) runWebSocket(ctx context.Context) {
	secret, err := a.appSecret()
	if err != nil {
		a.logger.Error("feishu websocket config error", "err", err)
		return
	}
	eventHandler := a.newEventDispatcher()
	bot.RunWithRetry(ctx, a.logger, "feishu sdk websocket", bot.RetryConfig{}, func(ctx context.Context) error {
		opts := []larkws.ClientOption{
			larkws.WithEventHandler(eventHandler),
			larkws.WithLogLevel(larkcore.LogLevelError),
			larkws.WithAutoReconnect(true),
			larkws.WithOnReady(func() { a.logger.Info("feishu sdk websocket connected") }),
			larkws.WithOnReconnecting(func() { a.logger.Warn("feishu sdk websocket reconnecting") }),
			larkws.WithOnReconnected(func() { a.logger.Info("feishu sdk websocket reconnected") }),
			larkws.WithOnError(func(err error) { a.logger.Error("feishu sdk websocket error", "err", err) }),
		}
		if feishuDomain(a.cfg.Domain) == "lark" {
			opts = append(opts, larkws.WithDomain(lark.LarkBaseUrl))
		}
		client := larkws.NewClient(a.cfg.AppID, secret, opts...)
		a.wsClient = client
		// client.Start blocks; run it off-loop so cancellation closes the client
		// immediately rather than waiting for Start to notice ctx. RunWithRetry
		// handles the reconnect backoff.
		errCh := make(chan error, 1)
		go func() { errCh <- client.Start(ctx) }()
		select {
		case <-ctx.Done():
			client.Close()
			return nil
		case err := <-errCh:
			client.Close()
			return err
		}
	})
}

func (a *adapter) newEventDispatcher() *dispatcher.EventDispatcher {
	return dispatcher.NewEventDispatcher(a.cfg.VerificationToken, "").
		OnP2MessageReceiveV1(func(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
			a.handleSDKMessage(event)
			return nil
		}).
		OnP2MessageReadV1(func(ctx context.Context, event *larkim.P2MessageReadV1) error {
			return nil
		}).
		OnP2MessageReactionCreatedV1(func(ctx context.Context, event *larkim.P2MessageReactionCreatedV1) error {
			return nil
		}).
		OnP2MessageReactionDeletedV1(func(ctx context.Context, event *larkim.P2MessageReactionDeletedV1) error {
			return nil
		}).
		OnP2CardActionTrigger(func(ctx context.Context, event *callback.CardActionTriggerEvent) (*callback.CardActionTriggerResponse, error) {
			if event == nil || event.EventReq == nil || !a.handleCardAction(event.Body) {
				a.logger.Warn("feishu card action ignored", "reason", "invalid_payload")
				return cardActionToast("warning", "操作无效或已过期"), nil
			}
			return cardActionToast("success", "操作已提交"), nil
		})
}

func (a *adapter) handleSDKMessage(event *larkim.P2MessageReceiveV1) {
	if event == nil || event.Event == nil || event.Event.Message == nil {
		return
	}
	eventID := ""
	if event.EventV2Base != nil && event.EventV2Base.Header != nil {
		eventID = event.EventV2Base.Header.EventID
	}
	if eventID != "" {
		if a.markSeen(eventID) {
			return
		}
	}
	msg := event.Event.Message
	msgType := stringPtrValue(msg.MessageType)
	decoded, reason, ok := a.decodeIncomingContent(context.Background(), stringPtrValue(msg.MessageId), msgType, stringPtrValue(msg.Content))
	if !ok {
		attrs := []any{"reason", reason, "msg_type", msgType, "chat_type", stringPtrValue(msg.ChatType), "message", logHash(stringPtrValue(msg.MessageId))}
		if strings.HasPrefix(reason, "post_") || strings.HasSuffix(reason, "_decode_failed") {
			attrs = append(attrs, "content", truncateContentForLog(stringPtrValue(msg.Content), 4000))
		}
		a.logger.Info("feishu message ignored", attrs...)
		return
	}
	chatType := bot.ChatDM
	if stringPtrValue(msg.ChatType) == "group" || stringPtrValue(msg.ChatType) == "topic_group" {
		chatType = bot.ChatGroup
		if a.cfg.RequireMention && len(msg.Mentions) == 0 && decoded.MentionCount == 0 {
			a.logger.Info("feishu message ignored", "reason", "missing_mention", "chat", logHash(stringPtrValue(msg.ChatId)), "message", logHash(stringPtrValue(msg.MessageId)))
			return
		}
	}
	userID := ""
	if event.Event.Sender != nil && event.Event.Sender.SenderId != nil {
		userID = firstNonEmpty(
			stringPtrValue(event.Event.Sender.SenderId.OpenId),
			stringPtrValue(event.Event.Sender.SenderId.UnionId),
			stringPtrValue(event.Event.Sender.SenderId.UserId),
		)
	}
	ib := bot.InboundMessage{
		Platform:  bot.PlatformFeishu,
		ChatType:  chatType,
		ChatID:    stringPtrValue(msg.ChatId),
		UserID:    userID,
		UserName:  userID,
		Text:      decoded.Text,
		MessageID: stringPtrValue(msg.MessageId),
		ThreadID:  stringPtrValue(msg.ThreadId),
		Media:     decoded.Media,
		Raw:       event,
	}
	select {
	case a.msgCh <- ib:
		a.logger.Info("feishu inbound queued", "chat_type", chatType, "chat", logHash(ib.ChatID), "user", logHash(ib.UserID), "message", logHash(ib.MessageID), "text_chars", len([]rune(ib.Text)))
	default:
		a.logger.Warn("feishu message channel full")
	}
}

func (a *adapter) handleWSEvent(ctx context.Context, raw json.RawMessage) {
	var evt feishuEvent
	if err := json.Unmarshal(raw, &evt); err != nil {
		return
	}

	if a.markSeen(evt.Header.EventID) {
		return
	}

	switch evt.Header.EventType {
	case "im.message.receive_v1":
		var msg feishuMsgEvent
		if err := json.Unmarshal(evt.Event, &msg); err != nil {
			return
		}
		a.handleMessage(msg)
	}
}

func truncateContentForLog(raw string, max int) string {
	raw = strings.TrimSpace(raw)
	if max <= 0 {
		return raw
	}
	r := []rune(raw)
	if len(r) <= max {
		return raw
	}
	return strings.TrimSpace(string(r[:max])) + "…"
}

func (a *adapter) decodeIncomingContent(ctx context.Context, messageID, msgType, raw string) (decodedIncomingContent, string, bool) {
	switch strings.TrimSpace(msgType) {
	case "text":
		var content textContent
		if err := json.Unmarshal([]byte(raw), &content); err != nil {
			return decodedIncomingContent{}, "text_decode_failed", false
		}
		return decodedIncomingContent{Text: strings.TrimSpace(content.Text)}, "", true
	case "post":
		return decodePostContent(raw)
	case "image":
		media, err := a.decodeImageContent(ctx, messageID, raw)
		if err != nil {
			return decodedIncomingContent{}, "image_decode_failed", false
		}
		return decodedIncomingContent{Media: []bot.InboundMedia{media}}, "", true
	case "file":
		media, err := a.decodeFileContent(ctx, messageID, raw)
		if err != nil {
			return decodedIncomingContent{}, "file_decode_failed", false
		}
		return decodedIncomingContent{Media: []bot.InboundMedia{media}}, "", true
	default:
		return decodedIncomingContent{}, "unsupported_msg_type", false
	}
}

func decodePostContent(raw string) (decodedIncomingContent, string, bool) {
	var content postContent
	if err := json.Unmarshal([]byte(raw), &content); err != nil {
		return decodedIncomingContent{}, "post_decode_failed", false
	}
	body := content.ZhCN
	if body == nil {
		body = content.EnUS
	}
	if body == nil {
		var direct postBody
		if err := json.Unmarshal([]byte(raw), &direct); err == nil && len(direct.Content) > 0 {
			body = &direct
		}
	}
	if body == nil {
		return decodedIncomingContent{}, "post_missing_locale", false
	}
	var parts []string
	mentionCount := 0
	for _, line := range body.Content {
		var lineParts []string
		for _, el := range line {
			switch el.Tag {
			case "text":
				if strings.TrimSpace(el.Text) != "" {
					lineParts = append(lineParts, el.Text)
				}
			case "at":
				mentionCount++
				name := strings.TrimSpace(el.UserName)
				if name != "" {
					lineParts = append(lineParts, "@"+name)
				}
			}
		}
		if len(lineParts) > 0 {
			parts = append(parts, strings.Join(lineParts, ""))
		}
	}
	text := strings.TrimSpace(strings.Join(parts, "\n"))
	if text == "" && mentionCount == 0 {
		return decodedIncomingContent{}, "post_empty_content", false
	}
	return decodedIncomingContent{Text: text, MentionCount: mentionCount}, "", true
}

func (a *adapter) decodeImageContent(ctx context.Context, messageID, raw string) (bot.InboundMedia, error) {
	var content imageContent
	if err := json.Unmarshal([]byte(raw), &content); err != nil {
		return bot.InboundMedia{}, err
	}
	if strings.TrimSpace(content.ImageKey) == "" {
		return bot.InboundMedia{}, fmt.Errorf("missing image_key")
	}
	return a.downloadInboundResource(ctx, messageID, content.ImageKey, "image")
}

func (a *adapter) decodeFileContent(ctx context.Context, messageID, raw string) (bot.InboundMedia, error) {
	var content fileContent
	if err := json.Unmarshal([]byte(raw), &content); err != nil {
		return bot.InboundMedia{}, err
	}
	if strings.TrimSpace(content.FileKey) == "" {
		return bot.InboundMedia{}, fmt.Errorf("missing file_key")
	}
	media, err := a.downloadInboundResource(ctx, messageID, content.FileKey, "file")
	if err != nil {
		return bot.InboundMedia{}, err
	}
	if strings.TrimSpace(media.Name) == "" {
		media.Name = strings.TrimSpace(content.FileName)
	}
	return media, nil
}

func (a *adapter) handleCardAction(raw []byte) bool {
	var payload struct {
		Header feishuHeader `json:"header"`
		Event  struct {
			Operator struct {
				UserID     string `json:"user_id"`
				OpenID     string `json:"open_id"`
				UnionID    string `json:"union_id"`
				OperatorID struct {
					UserID  string `json:"user_id"`
					OpenID  string `json:"open_id"`
					UnionID string `json:"union_id"`
				} `json:"operator_id"`
			} `json:"operator"`
			Context struct {
				OpenMessageID string `json:"open_message_id"`
				OpenChatID    string `json:"open_chat_id"`
			} `json:"context"`
			Action struct {
				Value map[string]string `json:"value"`
			} `json:"action"`
		} `json:"event"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	command := payload.Event.Action.Value["command"]
	if command == "" || payload.Event.Context.OpenChatID == "" {
		a.logger.Warn("feishu card action rejected", "reason", "missing_command_or_chat", "event_id", payload.Header.EventID != "")
		return false
	}
	if a.markSeen(payload.Header.EventID) {
		a.logger.Info("feishu card action deduped", "chat", logHash(payload.Event.Context.OpenChatID), "command", command)
		return true
	}
	chatType := cardActionChatType(payload.Event.Action.Value["chat_type"])
	operatorID := firstNonEmpty(
		payload.Event.Operator.OperatorID.UnionID,
		payload.Event.Operator.OperatorID.OpenID,
		payload.Event.Operator.OperatorID.UserID,
		payload.Event.Operator.UnionID,
		payload.Event.Operator.OpenID,
		payload.Event.Operator.UserID,
	)
	routeUserID := firstNonEmpty(payload.Event.Action.Value["user_id"], operatorID)
	a.logger.Info("feishu card action received", "chat_type", chatType, "chat", logHash(payload.Event.Context.OpenChatID), "message", logHash(payload.Event.Context.OpenMessageID), "operator", logHash(operatorID), "route_user", logHash(routeUserID), "command", command)
	ib := bot.InboundMessage{
		Platform:   bot.PlatformFeishu,
		ChatType:   chatType,
		ChatID:     payload.Event.Context.OpenChatID,
		UserID:     routeUserID,
		UserName:   routeUserID,
		OperatorID: operatorID,
		Text:       command,
		MessageID:  payload.Event.Context.OpenMessageID,
	}
	select {
	case a.msgCh <- ib:
		a.logger.Info("feishu card action queued", "chat", logHash(ib.ChatID), "message", logHash(ib.MessageID), "operator", logHash(operatorID), "command", command)
	default:
		a.logger.Warn("feishu card action channel full", "chat", logHash(ib.ChatID), "message", logHash(ib.MessageID), "command", command)
	}
	return true
}

func (a *adapter) markSeen(eventID string) bool {
	if eventID == "" {
		return false
	}
	a.seenMu.Lock()
	defer a.seenMu.Unlock()
	if a.seen == nil {
		a.seen = make(map[string]bool)
	}
	if a.seen[eventID] {
		return true
	}
	a.seen[eventID] = true
	if len(a.seen) > 10000 {
		a.seen = make(map[string]bool)
		a.seen[eventID] = true
	}
	return false
}

func cardActionChatType(raw string) bot.ChatType {
	switch bot.ChatType(raw) {
	case bot.ChatDM, bot.ChatGroup, bot.ChatGuild, bot.ChatDirect, bot.ChatThread:
		return bot.ChatType(raw)
	default:
		return bot.ChatGroup
	}
}

func cardActionToast(toastType, content string) *callback.CardActionTriggerResponse {
	return &callback.CardActionTriggerResponse{
		Toast: &callback.Toast{
			Type:    toastType,
			Content: content,
		},
	}
}

func (a *adapter) verificationTokenValid(token string) bool {
	if a.cfg.VerificationToken == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(a.cfg.VerificationToken)) == 1
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func logHash(id string) string {
	if id == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])[:12]
}

func (a *adapter) handleMessage(msg feishuMsgEvent) {
	decoded, reason, ok := a.decodeIncomingContent(context.Background(), msg.MessageID, msg.MsgType, msg.Content)
	if !ok {
		attrs := []any{"reason", reason, "msg_type", msg.MsgType, "chat_type", msg.ChatType, "message", logHash(msg.MessageID)}
		if strings.HasPrefix(reason, "post_") || strings.HasSuffix(reason, "_decode_failed") {
			attrs = append(attrs, "content", truncateContentForLog(msg.Content, 4000))
		}
		a.logger.Info("feishu message ignored", attrs...)
		return
	}

	// @mention gating：仅在群聊中检查是否 @了 bot
	chatType := bot.ChatDM
	if msg.ChatType == "group" || msg.ChatType == "topic_group" {
		chatType = bot.ChatGroup
		if a.cfg.RequireMention && len(msg.Mentions) == 0 && decoded.MentionCount == 0 {
			a.logger.Info("feishu message ignored", "reason", "missing_mention", "chat", logHash(msg.ChatID), "message", logHash(msg.MessageID))
			return
		}
	}

	ib := bot.InboundMessage{
		Platform:  bot.PlatformFeishu,
		ChatType:  chatType,
		ChatID:    msg.ChatID,
		UserID:    msg.Sender.SenderID.OpenID,
		UserName:  "",
		Text:      decoded.Text,
		MessageID: msg.MessageID,
		Media:     decoded.Media,
	}

	// 获取用户信息填充用户名
	if msg.Sender.SenderID.OpenID != "" {
		ib.UserName = msg.Sender.SenderID.OpenID
	}

	select {
	case a.msgCh <- ib:
		a.logger.Info("feishu inbound queued", "chat_type", chatType, "chat", logHash(ib.ChatID), "user", logHash(ib.UserID), "message", logHash(ib.MessageID), "text_chars", len([]rune(ib.Text)))
	default:
		a.logger.Warn("feishu message channel full")
	}
}

// SendText sends an interactive card with markdown content to a Feishu/Lark chat_id using the SDK.
// It is used by the desktop settings panel as an actual connection test.
func SendText(ctx context.Context, cfg config.FeishuBotConfig, chatID, text string) (bot.SendResult, error) {
	a := &adapter{cfg: cfg, logger: slog.Default().With("platform", "feishu")}
	return a.sendMessage(ctx, bot.OutboundMessage{ChatID: chatID, Text: text})
}

// sendMessage 使用飞书/Lark SDK 以 Interactive Card (JSON 2.0) 发送消息。
// Card 内嵌 markdown 元素，支持 CommonMark 标准语法。
// 当卡片体积超过 30KB 限制（如大段代码），自动降级为纯文本消息。
func (a *adapter) sendMessage(ctx context.Context, msg bot.OutboundMessage) (bot.SendResult, error) {
	if msg.Card != nil {
		return a.sendCard(ctx, msg)
	}
	if msg.Attachment != nil {
		return a.sendStructuredAttachment(ctx, msg, msg.Attachment)
	}
	return a.sendTextContent(ctx, msg, msg.Text)
}

func (a *adapter) sendStructuredAttachment(ctx context.Context, msg bot.OutboundMessage, att *bot.OutboundAttachment) (bot.SendResult, error) {
	if att == nil {
		return bot.SendResult{}, fmt.Errorf("feishu attachment is nil")
	}
	ref := strings.TrimSpace(att.Path)
	if ref == "" {
		return bot.SendResult{}, fmt.Errorf("feishu attachment path is empty")
	}
	return a.sendAttachmentRef(ctx, msg, ref, strings.TrimSpace(att.Name))
}

func (a *adapter) sendTextContent(ctx context.Context, msg bot.OutboundMessage, text string) (bot.SendResult, error) {
	if shouldUseFeishuPost(text) {
		postContent, err := buildPostMessage(strings.TrimSpace(a.cfg.PostTitle), text)
		if err == nil {
			return a.sendSDKContent(ctx, msg, larkim.MsgTypePost, postContent)
		}
		a.logger.Warn("build feishu post failed, falling back to markdown/text", "err", err)
	}
	cardContent, err := buildMarkdownCard(text)
	if err != nil {
		a.logger.Warn("build markdown card failed, falling back to text", "err", err)
		return a.sendSDKContent(ctx, msg, larkim.MsgTypeText, feishuTextContent(text))
	}
	result, err := a.sendSDKContent(ctx, msg, larkim.MsgTypeInteractive, cardContent)
	if err != nil && isCardLimitError(err) {
		a.logger.Warn("card send failed (size limit), retrying as text", "err", err)
		return a.sendSDKContent(ctx, msg, larkim.MsgTypeText, feishuTextContent(text))
	}
	return result, err
}

func buildMarkdownCard(content string) (string, error) {
	card := map[string]any{
		"schema": "2.0",
		"body": map[string]any{
			"elements": []map[string]any{
				{
					"tag":     "markdown",
					"content": content,
				},
			},
		},
	}
	data, err := json.Marshal(card)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func buildPostMessage(title, content string) (string, error) {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	lines := strings.Split(content, "\n")
	rows := make([][]postElement, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimRight(line, " \t")
		rows = append(rows, []postElement{{Tag: "text", Text: line}})
	}
	if len(rows) == 0 {
		rows = append(rows, []postElement{{Tag: "text", Text: ""}})
	}
	body := postContent{
		ZhCN: &postBody{Title: title, Content: rows},
		EnUS: &postBody{Title: title, Content: rows},
	}
	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func feishuTextContent(text string) string {
	content, _ := json.Marshal(textContent{Text: text})
	return string(content)
}

func isCardLimitError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "11310") || strings.Contains(s, "11325")
}

func shouldUseFeishuPost(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	markdownHints := []string{"```", "**", "__", "~~", "# ", "> ", "- [", "!["}
	for _, hint := range markdownHints {
		if strings.Contains(text, hint) {
			return false
		}
	}
	return strings.Contains(text, "\n")
}

func (a *adapter) sdkClient() (*lark.Client, error) {
	if a.client != nil {
		return a.client, nil
	}
	secret, err := a.appSecret()
	if err != nil {
		return nil, err
	}
	opts := []lark.ClientOptionFunc{
		lark.WithLogLevel(larkcore.LogLevelError),
		lark.WithReqTimeout(15 * time.Second),
		lark.WithSource("reasonix"),
	}
	if feishuDomain(a.cfg.Domain) == "lark" {
		opts = append(opts, lark.WithOpenBaseUrl(lark.LarkBaseUrl), lark.WithOAuthBaseUrl(lark.OAuthBaseUrlLark))
	}
	a.client = lark.NewClient(a.cfg.AppID, secret, opts...)
	return a.client, nil
}

func (a *adapter) uploadOutboundImage(ctx context.Context, raw []byte) (string, error) {
	if a.uploadImage != nil {
		return a.uploadImage(ctx, raw)
	}
	client, err := a.sdkClient()
	if err != nil {
		return "", err
	}
	req := larkim.NewCreateImageReqBuilder().
		Body(larkim.NewCreateImageReqBodyBuilder().
			ImageType("message").
			Image(bytes.NewReader(raw)).
			Build()).
		Build()
	resp, err := client.Im.Image.Create(ctx, req)
	if err != nil {
		return "", err
	}
	if resp == nil || !resp.Success() || resp.Data == nil || resp.Data.ImageKey == nil {
		if resp != nil {
			return "", fmt.Errorf("feishu image upload error: %s", feishuCodeError(resp.Code, resp.Msg))
		}
		return "", fmt.Errorf("feishu image upload error: empty response")
	}
	return strings.TrimSpace(*resp.Data.ImageKey), nil
}

func (a *adapter) uploadOutboundFile(ctx context.Context, fileName string, raw []byte) (string, error) {
	if a.uploadFile != nil {
		return a.uploadFile(ctx, fileName, raw)
	}
	client, err := a.sdkClient()
	if err != nil {
		return "", err
	}
	req := larkim.NewCreateFileReqBuilder().
		Body(&larkim.CreateFileReqBody{
			FileType: stringPtr("stream"),
			FileName: stringPtr(strings.TrimSpace(fileName)),
			File:     bytes.NewReader(raw),
		}).
		Build()
	resp, err := client.Im.File.Create(ctx, req)
	if err != nil {
		return "", err
	}
	if resp == nil || !resp.Success() || resp.Data == nil || resp.Data.FileKey == nil {
		if resp != nil {
			return "", fmt.Errorf("feishu file upload error: %s", feishuCodeError(resp.Code, resp.Msg))
		}
		return "", fmt.Errorf("feishu file upload error: empty response")
	}
	return strings.TrimSpace(*resp.Data.FileKey), nil
}

func (a *adapter) downloadInboundResource(ctx context.Context, messageID, fileKey, resourceType string) (bot.InboundMedia, error) {
	if a.download != nil {
		return a.download(ctx, messageID, fileKey, resourceType)
	}
	client, err := a.sdkClient()
	if err != nil {
		return bot.InboundMedia{}, err
	}
	req := larkim.NewGetMessageResourceReqBuilder().
		MessageId(strings.TrimSpace(messageID)).
		FileKey(strings.TrimSpace(fileKey)).
		Type(strings.TrimSpace(resourceType)).
		Build()
	resp, err := client.Im.MessageResource.Get(ctx, req)
	if err != nil {
		return bot.InboundMedia{}, err
	}
	if resp == nil {
		return bot.InboundMedia{}, fmt.Errorf("feishu message resource error: empty response")
	}
	if !resp.Success() && resp.StatusCode != http.StatusOK {
		return bot.InboundMedia{}, fmt.Errorf("feishu message resource error: %s", feishuCodeError(resp.Code, resp.Msg))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.File, feishuMaxInboundResourceBytes+1))
	if err != nil {
		return bot.InboundMedia{}, err
	}
	if len(raw) == 0 || len(raw) > feishuMaxInboundResourceBytes {
		return bot.InboundMedia{}, fmt.Errorf("media must be between 1 byte and 25 MB")
	}
	return bot.InboundMedia{
		Name:        strings.TrimSpace(resp.FileName),
		ContentType: strings.TrimSpace(resp.Header.Get("Content-Type")),
		Data:        raw,
	}, nil
}

func (a *adapter) sendSDKContent(ctx context.Context, msg bot.OutboundMessage, msgType, content string) (bot.SendResult, error) {
	if a.sendContent != nil {
		return a.sendContent(ctx, msg, msgType, content)
	}
	client, err := a.sdkClient()
	if err != nil {
		return bot.SendResult{}, err
	}
	chatID := strings.TrimSpace(msg.ChatID)
	if chatID == "" {
		return bot.SendResult{}, fmt.Errorf("feishu chat_id is empty")
	}
	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(larkim.CreateMessageV1ReceiveIDTypeChatId).
		Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(chatID).MsgType(msgType).Content(content).Build()).
		Build()
	resp, err := client.Im.Message.Create(ctx, req)
	if err != nil {
		return bot.SendResult{}, err
	}
	if resp == nil {
		return bot.SendResult{}, fmt.Errorf("feishu send error: empty response")
	}
	if !resp.Success() {
		return bot.SendResult{}, fmt.Errorf("feishu send error: %s", feishuCodeError(resp.Code, resp.Msg))
	}
	if resp.Data == nil {
		return bot.SendResult{}, nil
	}
	return bot.SendResult{MessageID: stringPtrValue(resp.Data.MessageId)}, nil
}

func (a *adapter) sendAttachmentRef(ctx context.Context, msg bot.OutboundMessage, ref string, preferredName string) (bot.SendResult, error) {
	name, raw, isImage, err := readWorkspaceFileRef(msg.WorkspaceRoot, ref)
	if err != nil {
		return bot.SendResult{}, err
	}
	if strings.TrimSpace(preferredName) != "" {
		name = strings.TrimSpace(preferredName)
	}
	if isImage {
		imageKey, err := a.uploadOutboundImage(ctx, raw)
		if err != nil {
			return bot.SendResult{}, err
		}
		payload, _ := json.Marshal(imageContent{ImageKey: imageKey})
		return a.sendSDKContent(ctx, msg, larkim.MsgTypeImage, string(payload))
	}
	fileKey, err := a.uploadOutboundFile(ctx, name, raw)
	if err != nil {
		return bot.SendResult{}, err
	}
	payload, _ := json.Marshal(fileContent{FileKey: fileKey, FileName: name})
	return a.sendSDKContent(ctx, msg, larkim.MsgTypeFile, string(payload))
}

func (a *adapter) AddPendingReaction(ctx context.Context, messageID string) (func(), error) {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return nil, nil
	}
	client, err := a.sdkClient()
	if err != nil {
		return nil, err
	}
	req := larkim.NewCreateMessageReactionReqBuilder().
		MessageId(messageID).
		Body(larkim.NewCreateMessageReactionReqBodyBuilder().
			ReactionType(larkim.NewEmojiBuilder().EmojiType(feishuPendingReactionEmoji).Build()).
			Build()).
		Build()
	resp, err := client.Im.MessageReaction.Create(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil || !resp.Success() {
		if resp != nil {
			return nil, fmt.Errorf("feishu reaction error: %s", feishuCodeError(resp.Code, resp.Msg))
		}
		return nil, fmt.Errorf("feishu reaction error: empty response")
	}
	reactionID := ""
	if resp.Data != nil && resp.Data.ReactionId != nil {
		reactionID = *resp.Data.ReactionId
	}
	if reactionID == "" {
		return nil, nil
	}
	cleanup := func() {
		delReq := larkim.NewDeleteMessageReactionReqBuilder().
			MessageId(messageID).
			ReactionId(reactionID).
			Build()
		if _, err := client.Im.MessageReaction.Delete(context.Background(), delReq); err != nil {
			a.logger.Warn("feishu reaction cleanup failed", "message", logHash(messageID), "err", err)
		}
	}
	return cleanup, nil
}

// sendCard 发送 interactive card 消息（用于审批/问答）。
func (a *adapter) sendCard(ctx context.Context, msg bot.OutboundMessage) (bot.SendResult, error) {
	card := msg.Card

	elements := make([]map[string]interface{}, 0)
	for _, el := range card.Elements {
		item := map[string]interface{}{"tag": el.Tag}
		if el.Content != "" {
			item["content"] = el.Content
		}
		if actions, ok := el.Extra["actions"]; ok && el.Tag == "action" {
			item["actions"] = actions
		} else {
			for k, v := range el.Extra {
				item[k] = v
			}
		}
		elements = append(elements, item)
	}

	cardPayload := map[string]interface{}{
		"header": map[string]interface{}{
			"title": map[string]string{
				"tag":     "plain_text",
				"content": card.Header,
			},
		},
		"elements": elements,
	}

	cardJSON, _ := json.Marshal(cardPayload)
	return a.sendSDKContent(ctx, msg, larkim.MsgTypeInteractive, string(cardJSON))
}

func feishuDomain(domain string) string {
	if strings.EqualFold(strings.TrimSpace(domain), "lark") {
		return "lark"
	}
	return "feishu"
}

func stringPtrValue(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return strings.TrimSpace(*ptr)
}

func stringPtr(v string) *string { return &v }

func feishuCodeError(code int, msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		msg = "unknown error"
	}
	if code == 0 {
		return msg
	}
	return fmt.Sprintf("%s (code %d)", msg, code)
}

func readWorkspaceFileRef(workspaceRoot, ref string) (name string, raw []byte, isImage bool, err error) {
	ref = strings.TrimSpace(ref)
	if workspaceRoot == "" {
		workspaceRoot = "."
	}
	absRoot, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return "", nil, false, err
	}
	var absPath string
	if filepath.IsAbs(ref) {
		absPath = filepath.Clean(ref)
	} else {
		cleanRel := filepath.Clean(filepath.FromSlash(ref))
		if cleanRel == "." || cleanRel == "" {
			return "", nil, false, fmt.Errorf("file path is empty")
		}
		absPath = filepath.Join(absRoot, cleanRel)
	}
	absPath = filepath.Clean(absPath)
	relToRoot, err := filepath.Rel(absRoot, absPath)
	if err != nil || relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(filepath.Separator)) {
		return "", nil, false, fmt.Errorf("file path is outside workspace")
	}
	cur := absRoot
	for _, part := range strings.Split(relToRoot, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		cur = filepath.Join(cur, part)
		info, err := os.Lstat(cur)
		if err != nil {
			return "", nil, false, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", nil, false, fmt.Errorf("file path must not contain symlinks")
		}
	}
	info, err := os.Lstat(absPath)
	if err != nil {
		return "", nil, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", nil, false, fmt.Errorf("file path must not be a symlink")
	}
	if info.IsDir() || info.Size() <= 0 || info.Size() > feishuMaxInboundResourceBytes {
		return "", nil, false, fmt.Errorf("file must be between 1 byte and 25 MB")
	}
	f, err := os.Open(absPath)
	if err != nil {
		return "", nil, false, err
	}
	defer f.Close()
	raw, err = io.ReadAll(io.LimitReader(f, feishuMaxInboundResourceBytes+1))
	if err != nil {
		return "", nil, false, err
	}
	if len(raw) == 0 || len(raw) > feishuMaxInboundResourceBytes {
		return "", nil, false, fmt.Errorf("file must be between 1 byte and 25 MB")
	}
	ext := strings.ToLower(filepath.Ext(absPath))
	switch ext {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff", ".ico":
		isImage = true
	}
	return filepath.Base(absPath), raw, isImage, nil
}

// runWebhook 启动飞书 Webhook 模式。
func (a *adapter) runWebhook(ctx context.Context) {
	port := a.cfg.WebhookPort
	if port == 0 {
		port = 8080
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/feishu/event", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
		if err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		var challenge struct {
			Challenge string `json:"challenge"`
			Token     string `json:"token"`
			Type      string `json:"type"`
		}
		_ = json.Unmarshal(body, &challenge)
		if challenge.Type == "url_verification" {
			if !a.verificationTokenValid(challenge.Token) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]string{"challenge": challenge.Challenge}); err != nil {
				a.logger.Error("feishu challenge response error", "err", err)
			}
			return
		}

		var evt feishuEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		if !a.verificationTokenValid(evt.Header.Token) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		if !a.handleCardAction(body) {
			raw, _ := json.Marshal(evt)
			a.handleWSEvent(ctx, raw)
		}
		w.WriteHeader(200)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		if err := server.Shutdown(context.Background()); err != nil && err != http.ErrServerClosed {
			a.logger.Error("feishu webhook shutdown error", "err", err)
		}
	}()

	a.logger.Info("feishu webhook listening", "port", port)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		a.logger.Error("feishu webhook server error", "err", err)
	}
}
