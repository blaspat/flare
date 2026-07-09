// Package term provides terminal colour helpers for the Flare CLI.
// It auto-detects whether colours are supported (TTY + NO_COLOR env) and
// provides colour and styling helpers that degrade gracefully when disabled.
package term

import (
	"fmt"
	"os"
	"strings"
)

// ANSI escape constants.
const (
	Reset  = "\033[0m"
	Bold   = "\033[1m"
	Dim    = "\033[2m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Magenta = "\033[35m"
	Cyan   = "\033[36m"
)

// Colorf wraps a format string in ANSI colour if the terminal supports it.
func Colorf(color, format string, args ...any) string {
	msg := fmt.Sprintf(format, args...)
	if enabled() && color != "" {
		return color + msg + Reset
	}
	return msg
}

// Boldf returns bold text when the terminal supports it.
func Boldf(format string, args ...any) string {
	return Colorf(Bold, format, args...)
}

// Dimf returns dim (faint) text when the terminal supports it.
func Dimf(format string, args ...any) string {
	return Colorf(Dim, format, args...)
}

// Bullet returns a coloured bullet character (or plain "-" if colour disabled).
func Bullet() string {
	if enabled() {
		return Cyan + "●" + Reset
	}
	return "•"
}

// Check returns a green check mark when colour is enabled, or "[ok]" otherwise.
func Check() string {
	if enabled() {
		return Green + "✓" + Reset
	}
	return "[ok]"
}

// Cross returns a red cross mark when colour is enabled, or "[✗]" otherwise.
func Cross() string {
	if enabled() {
		return Red + "✗" + Reset
	}
	return "[✗]"
}

// Header formats a section header (bold + colour).
func Header(format string, args ...any) string {
	return Colorf(Cyan+Bold, format, args...)
}

// Label formats a label: value pair with the label in bold.
func Label(label, value string) string {
	l := label
	if enabled() {
		l = Bold + label + Reset
	}
	return l + value
}

// Code formats inline code-like text in yellow (or plain if disabled).
func Code(format string, args ...any) string {
	return Colorf(Yellow, format, args...)
}

// enabled lazily caches whether the terminal supports colour output.
var enabled = syncEnabled()

func syncEnabled() func() bool {
	var (
		once syncBool
		ok   bool
	)
	return func() bool {
		once.Do(func() {
			if os.Getenv("NO_COLOR") != "" {
				return
			}
			fi, err := os.Stdout.Stat()
			ok = err == nil && (fi.Mode()&os.ModeCharDevice) != 0
		})
		return ok
	}
}

// syncBool is a simple once bool.
type syncBool struct {
	done bool
}

func (s *syncBool) Do(f func()) {
	if !s.done {
		s.done = true
		f()
	}
}

// --- Banner ---

var banner = fmt.Sprintf(`%s
   __ _ _ __ ___ _ __ ___
  / _' | '__/ _ \ '__/ __|
 | (_| | | |  __/ |  \__ \
  \__,_|_|  \___|_|  |___/
   Edge Mesh Server%s`, Cyan, Reset)

// BannerASCII returns the Flare ASCII art banner with ANSI colour.
func BannerASCII() string {
	if enabled() {
		return banner + "\n"
	}
	return `
   __ _ _ __ ___ _ __ ___
  / _' | '__/ _ \ '__/ __|
 | (_| | | |  __/ |  \__ \
  \__,_|_|  \___|_|  |___/
   Edge Mesh Server
`
}

// --- Progress bar -----------------------------------------------------------

// ProgressBar writes a simple text progress bar to dst.
// It always clears the current line with \r before drawing.
type ProgressBar struct {
	total int
	width int
}

// NewProgressBar creates a progress bar for total steps using width characters.
func NewProgressBar(total, width int) *ProgressBar {
	if width <= 0 {
		width = 30
	}
	if total <= 0 {
		total = 1
	}
	return &ProgressBar{total: total, width: width}
}

// Render returns the progress bar string for the given current count.
// Returns an empty string when colour is disabled (caller should handle
// progress logging via slog instead).
func (p *ProgressBar) Render(current int) string {
	if !enabled() {
		return ""
	}
	ratio := float64(current) / float64(p.total)
	if ratio > 1 {
		ratio = 1
	}
	filled := int(ratio * float64(p.width))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", p.width-filled)
	pct := int(ratio * 100)

	// Colour the bar based on progress.
	colour := Red
	if pct > 50 {
		colour = Yellow
	}
	if pct > 80 {
		colour = Green
	}
	return fmt.Sprintf("\r%s %s%3d%%%s",
		colour+bar+Reset,
		Bold,
		pct,
		Reset,
	)
}

// RenderDiff returns the progress bar as a line-ready string suitable for fmt.Print.
func (p *ProgressBar) RenderDiff(old, total int) string {
	return "\r" + p.Render(total-old) + " "
}
