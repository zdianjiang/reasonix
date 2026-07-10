package bot

// ReplyPayload is the bot-facing structured reply model produced from the
// shared event stream before a platform adapter turns it into IM messages.
// This first slice keeps the model intentionally small: ordered text and
// attachment segments plus a final marker for end-of-turn flushes.
type ReplyPayload struct {
	Segments []ReplySegment
	Final    bool
}

// ReplySegment is one ordered part of a bot reply.
type ReplySegment struct {
	Text       string
	Attachment *OutboundAttachment
}
