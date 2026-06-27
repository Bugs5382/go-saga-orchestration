package engine

import (
	"testing"
	"time"
)

func TestParseSchedule_NextHourly(t *testing.T) {
	sched, err := ParseSchedule("0 * * * *") // top of every hour
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 6, 27, 12, 30, 0, 0, time.UTC)
	got := sched.Next(from)
	want := time.Date(2026, 6, 27, 13, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("Next = %s, want %s", got, want)
	}
}

func TestParseSchedule_Invalid(t *testing.T) {
	if _, err := ParseSchedule("not a cron"); err == nil {
		t.Fatal("expected error for invalid expression")
	}
}
