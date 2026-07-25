package schedule

import (
	"testing"
	"time"
)

func TestParseInterval(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{"", 0, false},
		{"5m", 5 * time.Minute, false},
		{"1h", time.Hour, false},
		{"30s", 30 * time.Second, false},
		{"60", 60 * time.Minute, false},
		{"1d", 0, true},
	}
	for _, c := range cases {
		got, err := parseInterval(c.in)
		if c.err {
			if err == nil {
				t.Errorf("parseInterval(%q) expected error, got %v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseInterval(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseInterval(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestParseTaskSchedule(t *testing.T) {
	cases := []struct {
		schedule string
		ok       bool
		kind     string
	}{
		{"daily@09:00", true, "daily"},
		{"weekly:mon,wed,fri@14:30", true, "weekly"},
		{"biweekly:mon@10:00", true, "biweekly"},
		{"monthly:15@08:00", true, "monthly"},
		{"yearly:1-1@00:00", true, "yearly"},
		{"1h", false, ""},
		{"invalid", false, ""},
	}
	for _, c := range cases {
		s, ok := parseTaskSchedule(c.schedule)
		if ok != c.ok {
			t.Errorf("parseTaskSchedule(%q) ok=%v, want %v", c.schedule, ok, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if s.kind != c.kind {
			t.Errorf("parseTaskSchedule(%q) kind=%q, want %q", c.schedule, s.kind, c.kind)
		}
	}
}

func TestComputeNextRunAtInterval(t *testing.T) {
	base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	task := ScheduledTask{
		Schedule:  "1h",
		CreatedAt: base.UnixMilli(),
	}
	got := computeNextRunAt(task, base)
	want := base.Add(time.Hour)
	if !got.Equal(want) {
		t.Errorf("computeNextRunAt = %v, want %v", got, want)
	}

	// With a LastRunAt in the past, next should be LastRunAt + interval.
	task.LastRunAt = base.Add(-30 * time.Minute).UnixMilli()
	got = computeNextRunAt(task, base)
	want = base.Add(30 * time.Minute)
	if !got.Equal(want) {
		t.Errorf("computeNextRunAt with LastRunAt = %v, want %v", got, want)
	}
}

func TestComputeNextRunAtDaily(t *testing.T) {
	base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	task := ScheduledTask{
		Schedule:  "daily@09:00",
		CreatedAt: base.UnixMilli(),
	}
	got := computeNextRunAt(task, base)
	want := time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("computeNextRunAt daily = %v, want %v", got, want)
	}

	base = time.Date(2026, 7, 24, 8, 0, 0, 0, time.UTC)
	got = computeNextRunAt(task, base)
	want = time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("computeNextRunAt daily same day = %v, want %v", got, want)
	}
}

func TestTaskDueAtInterval(t *testing.T) {
	base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	task := ScheduledTask{
		Schedule:  "1h",
		CreatedAt: base.UnixMilli(),
	}
	if !taskDueAt(task, base.Add(61*time.Minute)) {
		t.Error("expected due after 1h1m")
	}
	if taskDueAt(task, base.Add(30*time.Minute)) {
		t.Error("expected not due after 30m")
	}
}

func TestTaskDueAtDaily(t *testing.T) {
	base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	task := ScheduledTask{
		Schedule:  "daily@09:00",
		CreatedAt: base.Add(-48 * time.Hour).UnixMilli(),
		NextRunAt: time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC).UnixMilli(),
	}
	if !taskDueAt(task, base) {
		t.Error("expected daily task due after its NextRunAt")
	}
	task.NextRunAt = base.Add(time.Hour).UnixMilli()
	if taskDueAt(task, base) {
		t.Error("expected task not due before NextRunAt")
	}
}
