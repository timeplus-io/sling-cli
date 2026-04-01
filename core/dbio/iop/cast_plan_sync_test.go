package iop

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBufferReplayCastPlanSync verifies that the buffer-replay goroutine
// waits for the target cast plan before processing rows. Without the fix,
// the first buffered rows would race through CastRow (which applies
// max_decimals truncation) instead of CastRowToTarget.
//
// The test simulates the real flow:
//   1. Create a datastream with a CSV-like iterator
//   2. Call Start() which buffers rows and starts the replay goroutine
//   3. The goroutine should wait for castPlanReady/pauseChan
//   4. Set the target cast plan (simulating ApplyTargetCastPlan)
//   5. Signal castPlanReady
//   6. Verify ALL rows (including the first) went through CastRowToTarget
func TestBufferReplayCastPlanSync(t *testing.T) {
	// Create columns matching a float64 target schema
	cols := Columns{
		{Name: "id", Type: IntegerType, Position: 1, DbType: "int32"},
		{Name: "val", Type: FloatType, Position: 2, DbType: "float64"},
	}

	// Track which cast path each row takes
	var mu sync.Mutex
	castPaths := map[int]string{} // row index → "fast" or "slow"

	// Create datastream with a simple iterator that produces string rows
	// (simulating CSV input)
	rows := [][]any{
		{"1", "6.75484118612095"},
		{"2", "9.87654321098765"},
		{"3", "3.14159265358979"},
	}
	rowIdx := 0
	ds := NewDatastreamContext(context.Background(), cols)
	ds.Sp.Config.MaxDecimals = 11
	ds.Sp.Config.maxDecimalsFormat = "%.11f"

	nextFunc := func(it *Iterator) bool {
		if rowIdx >= len(rows) {
			return false
		}
		it.Row = rows[rowIdx]
		rowIdx++
		return true
	}
	ds.it = ds.NewIterator(cols, nextFunc)

	// Start the datastream — this buffers all 3 rows (SampleSize > 3),
	// infers types, calls SetReady, and starts the replay goroutine.
	// The goroutine should block on castPlanReady before processing.
	err := ds.Start()
	require.NoError(t, err)

	// Wait for the stream to be ready
	err = ds.WaitReady()
	require.NoError(t, err)

	// At this point, the goroutine is started but should be waiting
	// on castPlanReady or pauseChan before processing buffer rows.
	// Simulate the dataflow adopting this stream.
	df := NewDataflow()
	ds.df = df

	// Now set the target cast plan (simulating ApplyTargetCastPlan)
	targetCols := Columns{
		{Name: "id", Type: IntegerType, Position: 1, DbType: "int32"},
		{Name: "val", Type: FloatType, Position: 2, DbType: "float64"},
	}
	ds.Sp.SetTargetCastPlan(targetCols)

	// Signal that the plan is ready — this unblocks the goroutine
	ds.SignalCastPlanReady()

	// Collect all rows from batches and check that the plan was used.
	// We verify by checking the types: CastRowToTarget returns typed
	// values (float64), while CastRow with max_decimals returns strings.
	timeout := time.After(5 * time.Second)
	collected := 0
	for collected < len(rows) {
		select {
		case batch, ok := <-ds.BatchChan:
			if !ok {
				t.Fatal("BatchChan closed before all rows collected")
			}
			for row := range batch.Rows {
				idx := collected
				val := row[1]
				mu.Lock()
				switch val.(type) {
				case float64:
					castPaths[idx] = "fast"
				case string:
					castPaths[idx] = "slow"
				default:
					castPaths[idx] = fmt.Sprintf("unknown(%T)", val)
				}
				mu.Unlock()
				collected++
			}
		case <-timeout:
			t.Fatalf("timed out waiting for rows, got %d/%d", collected, len(rows))
		}
	}

	// ALL rows should have gone through the fast path (CastRowToTarget)
	for i := 0; i < len(rows); i++ {
		assert.Equal(t, "fast", castPaths[i],
			"row %d should use CastRowToTarget (fast path), got %s", i, castPaths[i])
	}
}

// TestBufferReplayPauseHandshake verifies that the buffer-replay goroutine
// correctly handles the Pause/Unpause cycle that writeDirectly uses to set
// the cast plan. This is the exact sequence that caused the original bug:
//   1. Goroutine starts, waits on castPlanReady/pauseChan
//   2. TryPause sends to pauseChan (simulating df.Pause())
//   3. ApplyTargetCastPlan sets the plan + signals castPlanReady
//   4. Unpause sends to unpauseChan
//   5. Goroutine resumes and processes rows with the plan
func TestBufferReplayPauseHandshake(t *testing.T) {
	cols := Columns{
		{Name: "id", Type: IntegerType, Position: 1, DbType: "int32"},
		{Name: "val", Type: FloatType, Position: 2, DbType: "float64"},
	}

	rows := [][]any{
		{"1", "1.23456789012345"},
		{"2", "9.87654321098765"},
	}
	rowIdx := 0
	ds := NewDatastreamContext(context.Background(), cols)
	ds.Sp.Config.MaxDecimals = 11
	ds.Sp.Config.maxDecimalsFormat = "%.11f"

	nextFunc := func(it *Iterator) bool {
		if rowIdx >= len(rows) {
			return false
		}
		it.Row = rows[rowIdx]
		rowIdx++
		return true
	}
	ds.it = ds.NewIterator(cols, nextFunc)

	err := ds.Start()
	require.NoError(t, err)
	err = ds.WaitReady()
	require.NoError(t, err)

	// Simulate PushStreamChan setting ds.df
	df := NewDataflow()
	ds.df = df

	// Simulate df.Pause() — TryPause sends to pauseChan
	paused := ds.TryPause()
	require.True(t, paused, "TryPause should succeed while goroutine waits")

	// Simulate ApplyTargetCastPlan
	targetCols := Columns{
		{Name: "id", Type: IntegerType, Position: 1, DbType: "int32"},
		{Name: "val", Type: FloatType, Position: 2, DbType: "float64"},
	}
	ds.Sp.SetTargetCastPlan(targetCols)
	ds.SignalCastPlanReady()

	// Simulate Unpause — unblock the goroutine
	ds.Unpause()

	// Collect rows and verify they all used the fast path
	timeout := time.After(5 * time.Second)
	collected := 0
	for collected < len(rows) {
		select {
		case batch, ok := <-ds.BatchChan:
			if !ok {
				t.Fatal("BatchChan closed before all rows collected")
			}
			for row := range batch.Rows {
				val := row[1]
				_, isFast := val.(float64)
				assert.True(t, isFast,
					"row %d: expected float64 (fast path), got %T = %v", collected, val, val)
				collected++
			}
		case <-timeout:
			t.Fatalf("timed out, got %d/%d rows", collected, len(rows))
		}
	}
}

// TestStandaloneStreamNoDeadlock verifies that standalone streams (no
// dataflow, no PushStreamChan) still work correctly without blocking.
// WaitReady (called by Collect) signals castPlanReady, unblocking the
// buffer-replay goroutine deterministically — no timing heuristic.
func TestStandaloneStreamNoDeadlock(t *testing.T) {
	cols := Columns{
		{Name: "id", Type: IntegerType, Position: 1},
		{Name: "name", Type: StringType, Position: 2},
	}

	rows := [][]any{{"1", "alice"}, {"2", "bob"}}
	rowIdx := 0
	ds := NewDatastreamContext(context.Background(), cols)

	nextFunc := func(it *Iterator) bool {
		if rowIdx >= len(rows) {
			return false
		}
		it.Row = rows[rowIdx]
		rowIdx++
		return true
	}
	ds.it = ds.NewIterator(cols, nextFunc)

	err := ds.Start()
	require.NoError(t, err)

	// Collect without any dataflow — WaitReady signals castPlanReady,
	// so this should complete almost instantly, not hang.
	start := time.Now()
	data, err := ds.Collect(0)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 2, len(data.Rows), "should collect all rows")
	assert.Less(t, elapsed, 500*time.Millisecond, "should complete quickly (no timing heuristic)")
}

// TestSignalAllCastPlanReady verifies that SignalAllCastPlanReady unblocks
// streams that don't have a target cast plan (non-Proton targets).
func TestSignalAllCastPlanReady(t *testing.T) {
	cols := Columns{
		{Name: "id", Type: IntegerType, Position: 1},
		{Name: "val", Type: StringType, Position: 2},
	}

	rows := [][]any{{"1", "hello"}, {"2", "world"}}
	rowIdx := 0
	ds := NewDatastreamContext(context.Background(), cols)

	nextFunc := func(it *Iterator) bool {
		if rowIdx >= len(rows) {
			return false
		}
		it.Row = rows[rowIdx]
		rowIdx++
		return true
	}
	ds.it = ds.NewIterator(cols, nextFunc)

	err := ds.Start()
	require.NoError(t, err)
	err = ds.WaitReady()
	require.NoError(t, err)

	// Simulate dataflow adoption without a cast plan (non-Proton target)
	df := NewDataflow()
	ds.df = df

	// Signal without setting any plan — should still unblock
	ds.SignalCastPlanReady()

	timeout := time.After(5 * time.Second)
	collected := 0
	for collected < len(rows) {
		select {
		case batch, ok := <-ds.BatchChan:
			if !ok {
				t.Fatal("BatchChan closed")
			}
			for range batch.Rows {
				collected++
			}
		case <-timeout:
			t.Fatalf("timed out, got %d/%d rows", collected, len(rows))
		}
	}
	assert.Equal(t, len(rows), collected)
}
