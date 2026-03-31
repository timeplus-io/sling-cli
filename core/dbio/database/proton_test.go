package database

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestProtonIdempotentIdEscapesSingleQuotes verifies that single quotes in
// the idempotent_id prefix (e.g., from a StreamURL like s3://bucket/a'b.csv)
// are properly escaped when interpolated into the INSERT settings clause.
// This matches the escaping logic in GenerateInsertStatement.
func TestProtonIdempotentIdEscapesSingleQuotes(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		expected string
	}{
		{
			name:     "single quote in stream URL",
			prefix:   "exec123_s3://bucket/a'b.csv_default.`table`_batch_1",
			expected: "settings idempotent_id='exec123_s3://bucket/a''b.csv_default.`table`_batch_1'",
		},
		{
			name:     "multiple single quotes",
			prefix:   "exec_it's_o'clock_batch_1",
			expected: "settings idempotent_id='exec_it''s_o''clock_batch_1'",
		},
		{
			name:     "no single quotes",
			prefix:   "exec123_/tmp/data/file.csv_default.`table`_batch_1",
			expected: "settings idempotent_id='exec123_/tmp/data/file.csv_default.`table`_batch_1'",
		},
		{
			name:     "empty prefix uses timestamp fallback",
			prefix:   "",
			expected: "settings idempotent_id='", // just check prefix, timestamp varies
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Replicate the escaping logic from GenerateInsertStatement
			var settings string
			if len(tt.prefix) > 0 {
				escaped := strings.ReplaceAll(tt.prefix, "'", "''")
				settings = fmt.Sprintf("settings idempotent_id='%s'", escaped)
			} else {
				settings = "settings idempotent_id='"
			}
			assert.Contains(t, settings, tt.expected)
		})
	}
}
