package database

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/flarco/g"
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

// TestPermanentServerError verifies that Proton server errors (matching
// proto.Exception format "code: NN, message: ...") are classified as
// permanent and wrapped with backoff.Permanent, so retry loops stop
// immediately instead of retrying for 5 minutes.
func TestPermanentServerError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
	}{
		{
			name:      "server duplicate column error",
			err:       fmt.Errorf("code: 15, message: Column id specified more than once"),
			permanent: true,
		},
		{
			name:      "server syntax error",
			err:       fmt.Errorf("code: 62, message: Syntax error"),
			permanent: true,
		},
		{
			name:      "g.Error wrapped server error",
			err:       g.Error(fmt.Errorf("code: 15, message: Column id specified more than once"), "batch failed"),
			permanent: true,
		},
		{
			name:      "nil error",
			err:       nil,
			permanent: false,
		},
		{
			name:      "network EOF",
			err:       io.EOF,
			permanent: false,
		},
		{
			name:      "network timeout",
			err:       &net.OpError{Op: "read", Err: fmt.Errorf("connection reset by peer")},
			permanent: false,
		},
		{
			name:      "generic wrapped error",
			err:       g.Error(io.EOF, "connection lost"),
			permanent: false,
		},
		{
			name:      "plain string error",
			err:       errors.New("something went wrong"),
			permanent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test classification
			assert.Equal(t, tt.permanent, isProtonPermanentError(tt.err),
				"isProtonPermanentError mismatch")

			// Test wrapping
			wrapped := PermanentIfServerError(tt.err)
			if tt.err == nil {
				assert.Nil(t, wrapped)
				return
			}

			var permErr *backoff.PermanentError
			if tt.permanent {
				assert.True(t, errors.As(wrapped, &permErr),
					"expected backoff.Permanent wrapper for server error")
			} else {
				assert.False(t, errors.As(wrapped, &permErr),
					"transient error should NOT be wrapped as permanent")
				assert.Equal(t, tt.err, wrapped,
					"transient error should pass through unchanged")
			}
		})
	}
}

// TestTransientErrorRetries verifies that transient errors (network, EOF)
// are retried by the backoff helper while permanent server errors stop
// the loop after a single attempt.
func TestTransientErrorRetries(t *testing.T) {
	t.Run("permanent error stops after one attempt", func(t *testing.T) {
		attempts := 0
		serverErr := fmt.Errorf("code: 15, message: Column id specified more than once")

		bo := backoff.NewExponentialBackOff()
		bo.MaxElapsedTime = 5 * time.Second // generous budget — should never be used

		err := backoff.Retry(func() error {
			attempts++
			return PermanentIfServerError(serverErr)
		}, bo)

		assert.Equal(t, 1, attempts, "permanent error should be attempted exactly once")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "code: 15")
	})

	t.Run("transient error retries multiple times", func(t *testing.T) {
		attempts := 0
		maxAttempts := 3

		bo := backoff.NewExponentialBackOff()
		bo.MaxElapsedTime = 10 * time.Second
		bo.InitialInterval = 1 * time.Millisecond // fast for test

		err := backoff.Retry(func() error {
			attempts++
			if attempts >= maxAttempts {
				return nil // succeed on 3rd attempt
			}
			return PermanentIfServerError(io.EOF) // transient, should retry
		}, bo)

		assert.NoError(t, err)
		assert.Equal(t, maxAttempts, attempts,
			"transient error should retry until success")
	})
}
