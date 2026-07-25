package schedule

import "context"

type schedulerKey struct{}

// WithScheduler attaches a Scheduler to the context.
func WithScheduler(ctx context.Context, s *Scheduler) context.Context {
	return context.WithValue(ctx, schedulerKey{}, s)
}

// FromContext retrieves the Scheduler from the context.
func FromContext(ctx context.Context) (*Scheduler, bool) {
	s, ok := ctx.Value(schedulerKey{}).(*Scheduler)
	return s, ok
}

// ChatContext carries the routing information for the current chat.
type ChatContext struct {
	Platform     string
	ConnectionID string
	Domain       string
	ChatType     string
	ChatID       string
	UserID       string
	ThreadID     string
	// Scheduled marks a synthetic turn started by the scheduler. Persistent
	// scheduling tools must not be usable from such a turn.
	Scheduled bool
}

type chatContextKey struct{}

// WithChatContext attaches a ChatContext to the context.
func WithChatContext(ctx context.Context, cc ChatContext) context.Context {
	return context.WithValue(ctx, chatContextKey{}, cc)
}

// ChatContextFromContext retrieves the ChatContext from the context.
func ChatContextFromContext(ctx context.Context) (ChatContext, bool) {
	cc, ok := ctx.Value(chatContextKey{}).(ChatContext)
	return cc, ok
}
