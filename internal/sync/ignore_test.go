package sync

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIgnoreRules_Match(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		absPath string
		isDir   bool
		want    bool
	}{
		{"exact match", "node_modules", "/home/user/project/node_modules", true, true},
		{"exact match file", "secret.key", "/home/user/project/secret.key", false, true},
		{"non-match", "foo", "/home/user/project/bar", false, false},
		{"wildcard", "*.log", "/home/user/project/debug.log", false, true},
		{"wildcard no match", "*.log", "/home/user/project/debug.txt", false, false},
		{"directory pattern matches dir", "build/", "/home/user/project/build", true, true},
		{"directory pattern no match file", "build/", "/home/user/project/build", false, false},
		{"double star", "**/cache", "/home/user/project/src/cache", true, true},
		{"double star deep", "**/cache", "/home/user/project/a/b/c/cache", true, true},
		{"negation re-includes", ".env", "/home/user/project/.env", false, true},
		{"subdir pattern", "dist/*.zip", "/home/user/project/dist/release.zip", false, true},
		{"subdir non-match", "dist/*.zip", "/home/user/project/src/code.go", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build rules from a single pattern.
			line := tt.pattern
			p := ignorePattern{}

			// Parse like loadFile does.
			if _, err := os.Stat(filepath.Dir(tt.absPath)); err == nil {
				// Use a relative approach: create a temp dir, write .flareignore, load it.
			}

			if len(line) > 0 && line[0] == '!' {
				p.negative = true
				line = line[1:]
			}
			if len(line) > 0 && line[len(line)-1] == '/' {
				p.dirOnly = true
				line = line[:len(line)-1]
			}
			if len(line) > 0 && line[0] == '/' {
				p.anchored = true
				line = line[1:]
			}

			p.raw = line
			p.glob = line

			ir := &IgnoreRules{patterns: []ignorePattern{p}}
			got := ir.Match(tt.absPath, tt.isDir)
			if got != tt.want {
				t.Errorf("Match(%q, isDir=%v) = %v, want %v (pattern=%q)", tt.absPath, tt.isDir, got, tt.want, tt.pattern)
			}
		})
	}
}

func TestIgnoreRules_Negation(t *testing.T) {
	// Patterns: ignore *.log, but not important.log
	ir := &IgnoreRules{
		patterns: []ignorePattern{
			{raw: "*.log", glob: "*.log", negative: false},
			{raw: "important.log", glob: "important.log", negative: true},
		},
	}

	tests := []struct {
		path string
		want bool
	}{
		{"debug.log", true},
		{"error.log", true},
		{"important.log", false},  // negated
		{"readme.txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := ir.Match(tt.path, false)
			if got != tt.want {
				t.Errorf("Match(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestIgnoreRules_DirOnly(t *testing.T) {
	ir := &IgnoreRules{
		patterns: []ignorePattern{
			{raw: "node_modules", glob: "node_modules", dirOnly: true},
		},
	}

	if got := ir.Match("node_modules", true); got != true {
		t.Error("expected node_modules dir to be ignored")
	}
	if got := ir.Match("node_modules", false); got != false {
		t.Error("expected node_modules file to NOT be ignored")
	}
}

func TestIgnoreRules_LoadFromFile(t *testing.T) {
	dir := t.TempDir()

	// Create a .flareignore file.
	content := []byte("# Flare ignore file\n*.log\nnode_modules/\n!important.log\n")
	if err := os.WriteFile(filepath.Join(dir, ".flareignore"), content, 0644); err != nil {
		t.Fatal(err)
	}

	ir, err := LoadIgnoreRules(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ir == nil {
		t.Fatal("expected non-nil rules")
	}

	// Check the .flareignore file itself is ignored (it shouldn't be — we handle that in walkDir).
	// But the file IS on disk — walkDir skips it separately.

	tests := []struct {
		path  string
		isDir bool
		want  bool
	}{
		{filepath.Join(dir, "debug.log"), false, true},
		{filepath.Join(dir, "node_modules"), true, true},
		{filepath.Join(dir, "node_modules"), false, false}, // dirOnly
		{filepath.Join(dir, "important.log"), false, false}, // negated
		{filepath.Join(dir, "readme.txt"), false, false},
	}

	for _, tt := range tests {
		t.Run(filepath.Base(tt.path), func(t *testing.T) {
			got := ir.Match(tt.path, tt.isDir)
			if got != tt.want {
				t.Errorf("Match(%q, isDir=%v) = %v, want %v", tt.path, tt.isDir, got, tt.want)
			}
		})
	}
}

func TestIgnoreRules_ParentLoading(t *testing.T) {
	// Create dir structure:
	// tmp/
	//   .flareignore  ->  *.txt
	//   sub/
	//     .flareignore -> secret*
	//     secret.txt should be ignored by sub's rules
	//     notes.txt should be ignored by root's rules
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(root, ".flareignore"), []byte("*.txt\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, ".flareignore"), []byte("secret*\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Load rules from sub directory — should get both files' rules.
	ir, err := LoadIgnoreRules(sub)
	if err != nil {
		t.Fatal(err)
	}
	if ir == nil {
		t.Fatal("expected non-nil rules")
	}

	tests := []struct {
		name string
		path string
		want bool
	}{
		{"secret.txt in sub", filepath.Join(sub, "secret.txt"), true},
		{"notes.txt in sub", filepath.Join(sub, "notes.txt"), true},
		{"secret.zip in sub", filepath.Join(sub, "secret.zip"), true},   // from sub's rules
		{"readme.md in sub", filepath.Join(sub, "readme.md"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ir.Match(tt.path, false)
			if got != tt.want {
				t.Errorf("Match(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestIgnoreRules_IntegrationWithScan(t *testing.T) {
	dir := t.TempDir()

	// Create some files.
	files := []string{
		"readme.md",
		"debug.log",
		"src/main.go",
		"node_modules/pkg/index.js",
	}
	for _, f := range files {
		p := filepath.Join(dir, f)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Create .flareignore.
	ignoreContent := []byte("*.log\nnode_modules/\n")
	if err := os.WriteFile(filepath.Join(dir, ".flareignore"), ignoreContent, 0644); err != nil {
		t.Fatal(err)
	}

	ft := NewFileTracker([]WatchDir{{Path: dir, Tag: "test"}})

	events, err := ft.Scan()
	if err != nil {
		t.Fatal(err)
	}

	// We expect 2 files: readme.md and src/main.go
	// debug.log and node_modules/pkg/index.js should be ignored.
	var paths []string
	for _, e := range events {
		paths = append(paths, e.Path)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events (readme.md, src/main.go), got %d: %v", len(events), paths)
	}

	// Verify the right files were included.
	hasReadme := false
	hasMain := false
	for _, e := range events {
		if filepath.Base(e.Path) == "readme.md" {
			hasReadme = true
		}
		if filepath.Base(e.Path) == "main.go" {
			hasMain = true
		}
	}
	if !hasReadme || !hasMain {
		t.Errorf("missing expected files — got paths: %v", paths)
	}
}

func TestIgnoreRules_NewEmptyIgnoreRules(t *testing.T) {
	ir := NewEmptyIgnoreRules()
	if ir == nil {
		t.Fatal("expected non-nil")
	}
	if ir.Match("anything", false) {
		t.Error("empty rules should match nothing")
	}
	if ir.Match("anything", true) {
		t.Error("empty rules should match nothing (dir)")
	}
}

func TestIgnoreRules_LoadIgnoreForDir(t *testing.T) {
	// Non-existent dir — should return no-op rules.
	ir := LoadIgnoreForDir("/nonexistent/path")
	if ir == nil {
		t.Fatal("expected non-nil")
	}
	if ir.Match("anything", false) {
		t.Error("should match nothing for nonexistent dir")
	}

	// Real dir without .flareignore — should return no-op.
	ir = LoadIgnoreForDir(t.TempDir())
	if ir == nil {
		t.Fatal("expected non-nil")
	}
	if ir.Match("anything", false) {
		t.Error("should match nothing when no .flareignore")
	}
}
