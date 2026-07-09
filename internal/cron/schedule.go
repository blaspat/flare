package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule represents a time schedule for a cron job.
// Next returns the next scheduled time after the given time.
type Schedule interface {
	Next(after time.Time) time.Time
}

// EverySchedule fires at a fixed interval.
type EverySchedule struct {
	Interval time.Duration
}

func (s *EverySchedule) Next(after time.Time) time.Time {
	return after.Add(s.Interval)
}

// CronSchedule represents a 5-field cron expression.
// Fields: minute (0-59), hour (0-23), day of month (1-31), month (1-12), day of week (0-6, 0=Sunday).
// Supports: * (all), */N (every N), N (specific), N,M,L (list), N-M (range).
type CronSchedule struct {
	minute [60]bool
	hour   [24]bool
	dom    [31]bool
	month  [12]bool
	dow    [7]bool
}

// ParseSchedule parses a schedule string and returns the appropriate Schedule.
// Supported formats:
//   - @every <duration>  (e.g. @every 30s, @every 5m)
//   - cron expressions: "minute hour dom month dow"
func ParseSchedule(spec string) (Schedule, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, fmt.Errorf("empty schedule spec")
	}

	if strings.HasPrefix(spec, "@every ") {
		d, err := time.ParseDuration(spec[len("@every "):])
		if err != nil {
			return nil, fmt.Errorf("parse @every duration: %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("@every duration must be positive, got %s", d)
		}
		return &EverySchedule{Interval: d}, nil
	}

	return ParseCron(spec)
}

// ParseCron parses a 5-field cron expression.
func ParseCron(expr string) (*CronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields, got %d: %q", len(fields), expr)
	}

	cs := &CronSchedule{}

	// Set defaults: mark all as matching.
	for i := range cs.minute {
		cs.minute[i] = true
	}
	for i := range cs.hour {
		cs.hour[i] = true
	}
	for i := range cs.dom {
		cs.dom[i] = true
	}
	for i := range cs.month {
		cs.month[i] = true
	}
	for i := range cs.dow {
		cs.dow[i] = true
	}

	if err := parseFieldInto(cs.minute[:], fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	}
	if err := parseFieldInto(cs.hour[:], fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	}
	if err := parseFieldInto(cs.dom[:], fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("day of month: %w", err)
	}
	if err := parseFieldInto(cs.month[:], fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}
	if err := parseFieldInto(cs.dow[:], fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("day of week: %w", err)
	}

	return cs, nil
}

// parseFieldInto parses a single cron field into the given bits slice.
func parseFieldInto(bits []bool, field string, min, max int) error {
	// Reset all to false first.
	for i := range bits {
		bits[i] = false
	}

	if field == "*" {
		for i := min; i <= max; i++ {
			bits[i-min] = true
		}
		return nil
	}

	// Step: */N, N-M/step, or N/step
	if strings.Contains(field, "/") {
		parts := strings.SplitN(field, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid step expression: %q", field)
		}
		stepVal, err := strconv.Atoi(parts[1])
		if err != nil {
			return fmt.Errorf("invalid step value: %q", parts[1])
		}
		if stepVal <= 0 {
			return fmt.Errorf("step must be positive, got %d", stepVal)
		}

		start, end := min, max
		rangeStr := parts[0]
		if rangeStr == "*" {
			// */N — every N from min to max
		} else if strings.Contains(rangeStr, "-") {
			rangeParts := strings.SplitN(rangeStr, "-", 2)
			start, err = strconv.Atoi(rangeParts[0])
			if err != nil {
				return fmt.Errorf("invalid range start: %q", rangeParts[0])
			}
			end, err = strconv.Atoi(rangeParts[1])
			if err != nil {
				return fmt.Errorf("invalid range end: %q", rangeParts[1])
			}
		} else {
			// Single value with step — start from that value
			start, err = strconv.Atoi(rangeStr)
			if err != nil {
				return fmt.Errorf("invalid value: %q", rangeStr)
			}
		}

		if start < min || end > max || start > end {
			return fmt.Errorf("range [%d-%d] out of bounds [%d-%d]", start, end, min, max)
		}

		for i := start; i <= end; i += stepVal {
			bits[i-min] = true
		}
		return nil
	}

	// Range: N-M
	if strings.Contains(field, "-") && !strings.Contains(field, ",") {
		parts := strings.SplitN(field, "-", 2)
		start, err := strconv.Atoi(parts[0])
		if err != nil {
			return fmt.Errorf("invalid range start: %q", parts[0])
		}
		end, err := strconv.Atoi(parts[1])
		if err != nil {
			return fmt.Errorf("invalid range end: %q", parts[1])
		}
		if start < min || end > max || start > end {
			return fmt.Errorf("range [%d-%d] out of bounds [%d-%d]", start, end, min, max)
		}
		for i := start; i <= end; i++ {
			bits[i-min] = true
		}
		return nil
	}

	// List: N,M,L
	if strings.Contains(field, ",") {
		vals := strings.Split(field, ",")
		for _, v := range vals {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				return fmt.Errorf("invalid list value: %q", v)
			}
			if n < min || n > max {
				return fmt.Errorf("value %d out of bounds [%d-%d]", n, min, max)
			}
			bits[n-min] = true
		}
		return nil
	}

	// Single value
	n, err := strconv.Atoi(field)
	if err != nil {
		return fmt.Errorf("invalid value: %q", field)
	}
	if n < min || n > max {
		return fmt.Errorf("value %d out of bounds [%d-%d]", n, min, max)
	}
	bits[n-min] = true
	return nil
}

// Next returns the next time after after that matches this cron schedule.
func (cs *CronSchedule) Next(after time.Time) time.Time {
	t := after.Truncate(time.Minute).Add(time.Minute)

	// Look ahead up to 2 years
	end := after.AddDate(2, 0, 0)

	for t.Before(end) {
		y, m, d := t.Date()
		monthIdx := int(m) - 1
		day := d
		hour := t.Hour()
		minute := t.Minute()
		dowIdx := int(t.Weekday())

		monthMatch := cs.month[monthIdx]
		domMatch := cs.dom[day-1]
		dowMatch := cs.dow[dowIdx]

		// Day matches: if both dom and dow are restricted (not all *),
		// either can match (OR semantics).
		// If only one is restricted, that one must match (AND semantics).
		dayMatch := domMatch && dowMatch
		domAll := allTrue(cs.dom[:])
		dowAll := allTrue(cs.dow[:])
		if !domAll && !dowAll {
			dayMatch = domMatch || dowMatch
		} else if !domAll {
			dayMatch = domMatch
		} else if !dowAll {
			dayMatch = dowMatch
		}

		if monthMatch && dayMatch && cs.hour[hour] && cs.minute[minute] {
			return time.Date(y, m, d, hour, minute, 0, 0, t.Location())
		}

		t = t.Add(time.Minute)
	}

	return time.Time{}
}

func allTrue(bits []bool) bool {
	for _, b := range bits {
		if !b {
			return false
		}
	}
	return true
}
