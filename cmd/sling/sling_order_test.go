package main_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
)

// TestOrderedFileMergeStdout verifies that when streaming from a folder of CSVs
// using a wildcard, rows are emitted in strict filename order.
func TestOrderedFileMergeStdout(t *testing.T) {
	bin := os.Getenv("SLING_BIN")
	if bin == "" {
		t.Fatalf("SLING_BIN environment variable is not set")
		return
	}

	// Prepare temp folder with 50 CSV files named tablename_YYYYMMDD.csv
	dir, err := os.MkdirTemp("", "sling_order_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create files tablename_20240101.csv .. tablename_20240150.csv
	for i := 1; i <= 50; i++ {
		fname := fmt.Sprintf("tablename_202401%02d.csv", i)
		path := filepath.Join(dir, fname)
		content := fmt.Sprintf("id,name\n%d,row_%02d\n", i, i)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("failed writing %s: %v", path, err)
		}
	}

	// Run sling to merge in order and print to stdout
	p, err := process.NewProc("bash")
	if !g.AssertNoError(t, err) {
		return
	}
	p.Capture = true
	p.WorkDir = "../.."
	bin = "cmd/sling/" + bin
	p.Print = true

    cmd := strings.Join([]string{
		"set -e",
		"shopt -s expand_aliases",
		fmt.Sprintf("alias sling=%s", bin),
        fmt.Sprintf("sling run --src-stream 'file://%s' --src-options '{ format: \"csv\" }' --stdout", dir),
	}, "\n")

	err = p.Run("-lc", cmd)
	if err != nil {
		t.Fatalf("command failed: %v\nstdout: %s\nstderr: %s", err, p.Stdout.String(), p.Stderr.String())
	}

	// Parse stdout and ensure rows 1..50 are in order (header first)
	lines := strings.Split(strings.TrimSpace(p.Stdout.String()), "\n")
	if len(lines) < 51 {
		t.Fatalf("expected at least 51 lines (1 header + 50 rows), got %d", len(lines))
	}

	// Verify header
	if lines[0] != "id,name" {
		t.Fatalf("unexpected header: %q", lines[0])
	}

	// Verify each subsequent line has ascending id and matching name
	for i := 1; i <= 50; i++ {
		expected := fmt.Sprintf("%d,row_%02d", i, i)
		if lines[i] != expected {
			t.Fatalf("unexpected line %d: got %q, want %q", i, lines[i], expected)
		}
	}
}


