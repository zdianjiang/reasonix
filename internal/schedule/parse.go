package schedule

import (
	"strings"
	"time"
)

// taskSchedule is the parsed form of a calendar rule like "daily@09:00".
type taskSchedule struct {
	kind   string
	days   []time.Weekday
	month  int
	day    int
	hour   int
	minute int
	ok     bool
}

// parseInterval converts a string like "5m", "1h", "30s" to time.Duration.
// Empty or invalid strings return 0 and an error.
func parseInterval(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	switch s[len(s)-1] {
	case 's', 'm', 'h':
		return time.ParseDuration(s)
	default:
		return time.ParseDuration(s + "m")
	}
}

// parseTaskSchedule extracts a calendar rule from the schedule string.
// Supported forms: "daily@09:00", "weekly:mon@09:00", "biweekly:mon@09:00",
// "monthly:15@09:00", "yearly:1-1@09:00".
func parseTaskSchedule(schedule string) (taskSchedule, bool) {
	schedule = strings.TrimSpace(schedule)
	idx := strings.Index(schedule, "|")
	if idx < 0 {
		idx = strings.Index(schedule, "@")
		if idx < 0 {
			return taskSchedule{}, false
		}
		// Form: "daily@09:00"
		raw := strings.TrimSpace(schedule[:idx])
		at := strings.TrimSpace(schedule[idx+1:])
		return parseCalendarRule(raw, at)
	}
	// Form: "1h|daily@09:00" — ignore interval, parse calendar part.
	raw := strings.TrimSpace(schedule[idx+1:])
	if raw == "" {
		return taskSchedule{}, false
	}
	at := "09:00"
	if parts := strings.SplitN(raw, "@", 2); len(parts) == 2 {
		raw = strings.TrimSpace(parts[0])
		at = strings.TrimSpace(parts[1])
	}
	return parseCalendarRule(raw, at)
}

func parseCalendarRule(raw, at string) (taskSchedule, bool) {
	hour, minute, ok := parseClock(at)
	if !ok {
		return taskSchedule{}, false
	}
	kind := raw
	rule := ""
	if parts := strings.SplitN(raw, ":", 2); len(parts) == 2 {
		kind = strings.TrimSpace(parts[0])
		rule = strings.TrimSpace(parts[1])
	}
	s := taskSchedule{kind: kind, hour: hour, minute: minute, ok: true}
	switch kind {
	case "daily":
		return s, true
	case "weekly", "biweekly":
		for _, part := range strings.Split(rule, ",") {
			if wd, ok := parseWeekday(part); ok {
				s.days = append(s.days, wd)
			}
		}
		return s, len(s.days) > 0
	case "monthly":
		s.day = parsePositiveInt(rule, 0)
		return s, s.day >= 1 && s.day <= 31
	case "yearly":
		parts := strings.SplitN(rule, "-", 2)
		if len(parts) != 2 {
			return taskSchedule{}, false
		}
		s.month = parsePositiveInt(parts[0], 0)
		s.day = parsePositiveInt(parts[1], 0)
		return s, s.month >= 1 && s.month <= 12 && s.day >= 1 && s.day <= 31
	default:
		return taskSchedule{}, false
	}
}

// taskDueAt reports whether the task should run at now.
func taskDueAt(t ScheduledTask, now time.Time) bool {
	if t.NextRunAt != 0 {
		return !now.Before(time.UnixMilli(t.NextRunAt))
	}
	// Fallback for tasks without NextRunAt: compute from LastRunAt/CreatedAt.
	if scheduled, ok := previousScheduleAt(t, now); ok {
		if t.CreatedAt != 0 && scheduled.Before(time.UnixMilli(t.CreatedAt)) {
			return false
		}
		if t.LastRunAt != 0 && !time.UnixMilli(t.LastRunAt).Before(scheduled) {
			return false
		}
		return !scheduled.After(now)
	}

	d, err := parseInterval(t.Schedule)
	if err != nil || d <= 0 {
		return false
	}
	baseMillis := t.LastRunAt
	if baseMillis == 0 {
		baseMillis = t.CreatedAt
	}
	if baseMillis == 0 {
		return true
	}
	return now.Sub(time.UnixMilli(baseMillis)) >= d
}

// computeNextRunAt returns the next scheduled time strictly after from.
func computeNextRunAt(t ScheduledTask, from time.Time) time.Time {
	from = from.In(taskLocation(t))
	if scheduled, ok := parseTaskSchedule(t.Schedule); ok {
		return nextScheduleAt(t, scheduled, from)
	}
	d, err := parseInterval(t.Schedule)
	if err != nil || d <= 0 {
		return from
	}
	var base time.Time
	if t.LastRunAt != 0 {
		base = time.UnixMilli(t.LastRunAt).Add(d)
	} else if t.CreatedAt != 0 {
		base = time.UnixMilli(t.CreatedAt).Add(d)
	} else {
		base = from.Add(d)
	}
	for base.Before(from) {
		base = base.Add(d)
	}
	return base
}

func previousScheduleAt(t ScheduledTask, now time.Time) (time.Time, bool) {
	s, ok := parseTaskSchedule(t.Schedule)
	if !ok {
		return time.Time{}, false
	}
	switch s.kind {
	case "daily":
		candidate := dateAt(now.Year(), now.Month(), now.Day(), s.hour, s.minute, now.Location())
		if candidate.After(now) {
			candidate = candidate.AddDate(0, 0, -1)
		}
		return candidate, true
	case "weekly":
		return previousWeeklyAt(s, now, 7, time.Time{})
	case "biweekly":
		anchor := scheduleAnchor(t, now)
		return previousWeeklyAt(s, now, 14, anchor)
	case "monthly":
		return previousMonthlyAt(s, now), true
	case "yearly":
		return previousYearlyAt(s, now), true
	default:
		return time.Time{}, false
	}
}

func nextScheduleAt(t ScheduledTask, s taskSchedule, from time.Time) time.Time {
	switch s.kind {
	case "daily":
		candidate := dateAt(from.Year(), from.Month(), from.Day(), s.hour, s.minute, from.Location())
		if candidate.Before(from) {
			candidate = candidate.AddDate(0, 0, 1)
		}
		return candidate
	case "weekly":
		return nextWeeklyAt(s, from, 7, time.Time{})
	case "biweekly":
		return nextWeeklyAt(s, from, 14, scheduleAnchor(t, from))
	case "monthly":
		return nextMonthlyAt(s, from)
	case "yearly":
		return nextYearlyAt(s, from)
	default:
		return from
	}
}

func taskLocation(t ScheduledTask) *time.Location {
	if tz := strings.TrimSpace(t.Timezone); tz != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			return loc
		}
	}
	return time.Local
}

func previousWeeklyAt(s taskSchedule, now time.Time, windowDays int, anchor time.Time) (time.Time, bool) {
	var best time.Time
	for offset := 0; offset < windowDays; offset++ {
		day := now.AddDate(0, 0, -offset)
		for _, wd := range s.days {
			if day.Weekday() != wd {
				continue
			}
			candidate := dateAt(day.Year(), day.Month(), day.Day(), s.hour, s.minute, now.Location())
			if candidate.After(now) {
				continue
			}
			if !anchor.IsZero() && weeksBetween(weekStart(anchor), weekStart(candidate))%2 != 0 {
				continue
			}
			if best.IsZero() || candidate.After(best) {
				best = candidate
			}
		}
	}
	return best, !best.IsZero()
}

func nextWeeklyAt(s taskSchedule, from time.Time, windowDays int, anchor time.Time) time.Time {
	var best time.Time
	for offset := 0; offset < windowDays+7; offset++ {
		day := from.AddDate(0, 0, offset)
		for _, wd := range s.days {
			if day.Weekday() != wd {
				continue
			}
			candidate := dateAt(day.Year(), day.Month(), day.Day(), s.hour, s.minute, from.Location())
			if candidate.Before(from) {
				continue
			}
			if !anchor.IsZero() && weeksBetween(weekStart(anchor), weekStart(candidate))%2 != 0 {
				continue
			}
			if best.IsZero() || candidate.Before(best) {
				best = candidate
			}
		}
	}
	if best.IsZero() {
		return from
	}
	return best
}

func previousMonthlyAt(s taskSchedule, now time.Time) time.Time {
	candidate := monthlyCandidate(now.Year(), now.Month(), s.day, s.hour, s.minute, now.Location())
	if candidate.After(now) {
		prev := now.AddDate(0, -1, 0)
		candidate = monthlyCandidate(prev.Year(), prev.Month(), s.day, s.hour, s.minute, now.Location())
	}
	return candidate
}

func nextMonthlyAt(s taskSchedule, from time.Time) time.Time {
	candidate := monthlyCandidate(from.Year(), from.Month(), s.day, s.hour, s.minute, from.Location())
	if candidate.Before(from) {
		next := from.AddDate(0, 1, 0)
		candidate = monthlyCandidate(next.Year(), next.Month(), s.day, s.hour, s.minute, from.Location())
	}
	return candidate
}

func previousYearlyAt(s taskSchedule, now time.Time) time.Time {
	month := time.Month(s.month)
	candidate := monthlyCandidate(now.Year(), month, s.day, s.hour, s.minute, now.Location())
	if candidate.After(now) {
		candidate = monthlyCandidate(now.Year()-1, month, s.day, s.hour, s.minute, now.Location())
	}
	return candidate
}

func nextYearlyAt(s taskSchedule, from time.Time) time.Time {
	month := time.Month(s.month)
	candidate := monthlyCandidate(from.Year(), month, s.day, s.hour, s.minute, from.Location())
	if candidate.Before(from) {
		candidate = monthlyCandidate(from.Year()+1, month, s.day, s.hour, s.minute, from.Location())
	}
	return candidate
}

func scheduleAnchor(t ScheduledTask, now time.Time) time.Time {
	if t.CreatedAt != 0 {
		return time.UnixMilli(t.CreatedAt)
	}
	if t.LastRunAt != 0 {
		return time.UnixMilli(t.LastRunAt)
	}
	return now
}

func parseClock(s string) (int, int, bool) {
	parts := strings.SplitN(strings.TrimSpace(s), ":", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	hour := parsePositiveInt(parts[0], -1)
	minute := parsePositiveInt(parts[1], -1)
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, 0, false
	}
	return hour, minute, true
}

func parseWeekday(s string) (time.Weekday, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "sun", "sunday":
		return time.Sunday, true
	case "mon", "monday":
		return time.Monday, true
	case "tue", "tuesday":
		return time.Tuesday, true
	case "wed", "wednesday":
		return time.Wednesday, true
	case "thu", "thursday":
		return time.Thursday, true
	case "fri", "friday":
		return time.Friday, true
	case "sat", "saturday":
		return time.Saturday, true
	default:
		return time.Sunday, false
	}
}

func parsePositiveInt(s string, fallback int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return fallback
		}
		n = n*10 + int(r-'0')
	}
	return n
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func dateAt(year int, month time.Month, day, hour, minute int, loc *time.Location) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, loc)
}

func monthlyCandidate(year int, month time.Month, day, hour, minute int, loc *time.Location) time.Time {
	if day < 1 {
		day = 1
	}
	if max := daysInMonth(year, month, loc); day > max {
		day = max
	}
	return dateAt(year, month, day, hour, minute, loc)
}

func daysInMonth(year int, month time.Month, loc *time.Location) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, loc).Day()
}

func weekStart(t time.Time) time.Time {
	dayOffset := (int(t.Weekday()) + 6) % 7
	base := dateAt(t.Year(), t.Month(), t.Day(), 0, 0, t.Location())
	return base.AddDate(0, 0, -dayOffset)
}

func weeksBetween(a, b time.Time) int {
	if b.Before(a) {
		a, b = b, a
	}
	return int(b.Sub(a).Hours() / 24 / 7)
}
