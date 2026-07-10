// Package sync provides file-system tracking and synchronisation for the Flare
// edge mesh.
package sync

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// --- IgnoreRules ------------------------------------------------------------

// IgnoreRules holds a compiled set of gitignore-style patterns loaded from
// .flareignore files. Patterns are evaluated in order; the last matching
// pattern wins (so a later negate pattern can re-include a file).
type IgnoreRules struct {
	patterns []ignorePattern
}

type ignorePattern struct {
	raw      string // the original line (minus # comments and leading/trailing ws)
	negative bool   // true for !pattern
	dirOnly  bool   // true for pattern/ (only matches directories)
	anchored bool   // true for /pattern or pattern/ (relative to .flareignore root)
	glob     string // compiled glob for matching
}

// LoadIgnoreRules walks up from dir looking for .flareignore files and
// merges them bottom-up (most-specific directory first). Each .flareignore
// file's patterns apply to its own directory and all subdirectories.
func LoadIgnoreRules(dir string) (*IgnoreRules, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	// Collect .flareignore files from dir up to root.
	var paths []string
	cur := absDir
	for {
		candidate := filepath.Join(cur, ".flareignore")
		if _, err := os.Stat(candidate); err == nil {
			paths = append(paths, candidate)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	// Reverse so most-specific dir is first.
	for i, j := 0, len(paths)-1; i < j; i, j = i+1, j-1 {
		paths[i], paths[j] = paths[j], paths[i]
	}

	ir := &IgnoreRules{}
	for _, p := range paths {
		if err := ir.loadFile(p); err != nil {
			return nil, err
		}
	}
	return ir, nil
}

// loadFile parses a single .flareignore file and appends its patterns.
func (ir *IgnoreRules) loadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	baseDir := filepath.Dir(path)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Blank lines and comments.
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		p := ignorePattern{}

		// Negation.
		if strings.HasPrefix(line, "!") {
			p.negative = true
			line = line[1:]
		}

		// Directory-only marker.
		if strings.HasSuffix(line, "/") {
			p.dirOnly = true
			line = strings.TrimSuffix(line, "/")
		}

		// Anchored? A pattern containing / (except trailing /) or starting
		// with / is treated as relative to the .flareignore file's directory.
		hasSlash := strings.Contains(line, "/")
		p.anchored = hasSlash && !strings.HasSuffix(line, "/")

		if strings.HasPrefix(line, "/") {
			p.anchored = true
			line = line[1:]
		}

		p.raw = line
		p.glob = filepath.Join(baseDir, line)
		if !p.anchored {
			// Unanchored patterns match anywhere in the tree.
			// We use **/pattern matching.
			p.glob = line
		}

		ir.patterns = append(ir.patterns, p)
	}
	return scanner.Err()
}

// Match checks whether a relative or absolute path should be ignored.
// isDir should be true when the path refers to a directory.
// Returns true if the path is ignored, false if it should be included.
func (ir *IgnoreRules) Match(absPath string, isDir bool) bool {
	if ir == nil || len(ir.patterns) == 0 {
		return false
	}

	matched := false
	for _, p := range ir.patterns {
		// Directory-only patterns don't match files.
		if p.dirOnly && !isDir {
			continue
		}

		if matchGlob(absPath, p.glob, p.anchored) {
			matched = !p.negative
		}
	}
	return matched
}

// matchGlob checks if path matches a gitignore-style glob pattern.
func matchGlob(path, pattern string, anchored bool) bool {
	// Simple cases first — fast path for exact/prefix/suffix matches.

	// If the pattern has no glob metacharacters, do exact match.
	if !containsGlobMeta(pattern) {
		if anchored {
			return path == pattern
		}
		// Unanchored: match any suffix.
		return strings.HasSuffix(path, pattern) || path == pattern
	}

	// For glob patterns, fall through to filepath.Match.
	// We handle ** ourselves since filepath.Match doesn't support it.
	return matchGlobRecursive(path, pattern, anchored)
}

// containsGlobMeta checks for glob metacharacters.
func containsGlobMeta(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

// matchGlobRecursive is a simple recursive matcher that supports **.
func matchGlobRecursive(path, pattern string, anchored bool) bool {
	// Split into segments.
	pathSegs := splitPath(path)
	patSegs := splitPath(pattern)

	if anchored {
		return matchSegments(pathSegs, patSegs)
	}
	// Unanchored: try matching at every suffix of path.
	for i := 0; i <= len(pathSegs); i++ {
		if matchSegments(pathSegs[i:], patSegs) {
			return true
		}
	}
	return false
}

// splitPath splits a file path into components.
func splitPath(p string) []string {
	if p == "" {
		return nil
	}
	return strings.Split(p, string(filepath.Separator))
}

// matchSegments matches path segments against pattern segments recursively.
func matchSegments(path, pattern []string) bool {
	pi, pp := 0, 0
	for pi < len(path) && pp < len(pattern) {
		pat := pattern[pp]
		if pat == "**" {
			// ** matches zero or more segments.
			// Try matching the rest of the pattern against every suffix.
			for i := pi; i <= len(path); i++ {
				if matchSegments(path[i:], pattern[pp+1:]) {
					return true
				}
			}
			return false
		}
		matched, err := filepath.Match(pat, path[pi])
		if err != nil || !matched {
			return false
		}
		pi++
		pp++
	}
	// All path segments consumed? All pattern segments consumed?
	if pi < len(path) {
		return false
	}
	// Remaining pattern segments must all be ** or empty.
	for _, p := range pattern[pp:] {
		if p != "**" {
			return false
		}
	}
	return true
}

// NewEmptyIgnoreRules returns a no-op IgnoreRules that matches nothing.
// Useful for tests and when no .flareignore files exist.
func NewEmptyIgnoreRules() *IgnoreRules {
	return &IgnoreRules{}
}

// LoadIgnoreForDir attempts to load .flareignore for a directory.
// Returns a no-op ruleset if no .flareignore exists or on error.
func LoadIgnoreForDir(dir string) *IgnoreRules {
	rules, err := LoadIgnoreRules(dir)
	if err != nil || rules == nil {
		return NewEmptyIgnoreRules()
	}
	return rules
}
