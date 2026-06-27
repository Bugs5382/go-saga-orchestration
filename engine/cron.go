package engine

import cron "github.com/robfig/cron/v3"

// ParseSchedule parses a standard 5-field cron expression (and @descriptors).
// Granularity is one minute; sub-minute cadences are out of scope.
func ParseSchedule(expr string) (cron.Schedule, error) {
	return cron.ParseStandard(expr)
}
