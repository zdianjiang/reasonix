package tool

import (
	"context"

	"reasonix/internal/event"
)

type eventSinkContextKey struct{}

// WithEventSink carries the active runtime event sink into tool execution so
// tools can emit structured frontend-only side channels such as reply media.
func WithEventSink(ctx context.Context, sink event.Sink) context.Context {
	return context.WithValue(ctx, eventSinkContextKey{}, sink)
}

// EventSinkFromContext returns the event sink previously attached with
// WithEventSink.
func EventSinkFromContext(ctx context.Context) (event.Sink, bool) {
	sink, ok := ctx.Value(eventSinkContextKey{}).(event.Sink)
	return sink, ok && sink != nil
}
