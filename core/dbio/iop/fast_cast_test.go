package iop

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestUnwrapDbType(t *testing.T) {
	sp := &StreamProcessor{Config: &StreamConfig{NullIf: "NULL"}, dateLayouts: []string{"2006-01-02"}}
	plan := NewTargetCastPlan(Columns{}, []string{}, sp)

	cases := []struct {
		col    Column
		input  string
		expect any
	}{
		{Column{DbType: "int64"}, "42", int64(42)},
		{Column{DbType: "nullable(float64)"}, "3.14", 3.14},
		{Column{DbType: "low_cardinality(string)"}, "hello", "hello"},
		{Column{DbType: "nullable(low_cardinality(string))"}, "world", "world"},
		{Column{DbType: "low_cardinality(nullable(string))"}, "test", "test"},
		{Column{DbType: "nullable(decimal(18, 2))"}, "99.99", mustDecimal("99.99")},
		{Column{DbType: "nullable(bool)"}, "true", true},
	}
	for _, c := range cases {
		parser := plan.makeParser(0, c.col)
		result, err := parser(c.input)
		assert.NoError(t, err, "dbType=%s input=%s", c.col.DbType, c.input)
		assert.Equal(t, c.expect, result, "dbType=%s input=%s", c.col.DbType, c.input)
	}
}

func mustDecimal(s string) any {
	v, _ := decimal.NewFromString(s)
	return v
}

func TestParsers(t *testing.T) {
	// bool
	v, err := parseBool("true")
	assert.NoError(t, err)
	assert.Equal(t, true, v)
	v, err = parseBool("false")
	assert.NoError(t, err)
	assert.Equal(t, false, v)
	v, err = parseBool("1")
	assert.NoError(t, err)
	assert.Equal(t, true, v)

	// int parsers
	v, err = parseInt64("12345")
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), v)
	// int64 has no float fallback — decimal-formatted values error
	_, err = parseInt64("123.0")
	assert.Error(t, err, "int64 should reject float-formatted input")

	v, err = parseInt32("-99")
	assert.NoError(t, err)
	assert.Equal(t, int32(-99), v)

	// uint parsers
	v, err = parseUint64("999")
	assert.NoError(t, err)
	assert.Equal(t, uint64(999), v)
	// float-formatted uint
	v, err = parseUint32("42.0")
	assert.NoError(t, err)
	assert.Equal(t, uint32(42), v)

	// float
	v, err = parseFloat64("3.14")
	assert.NoError(t, err)
	assert.Equal(t, 3.14, v)

	// decimal
	v, err = parseDecimal("123.456")
	assert.NoError(t, err)
	expected, _ := decimal.NewFromString("123.456")
	assert.Equal(t, expected, v)

	// identity
	v, err = parseIdentity("hello world")
	assert.NoError(t, err)
	assert.Equal(t, "hello world", v)
}

func TestCastRowNullSemantics(t *testing.T) {
	cols := Columns{
		{Name: "str_col", Type: StringType, DbType: "string"},
		{Name: "int_col", Type: BigIntType, DbType: "int64"},
		{Name: "float_col", Type: FloatType, DbType: "float64"},
	}
	sp := &StreamProcessor{
		Config: &StreamConfig{
			TrimSpace:   false,
			EmptyAsNull: false,
			NullIf:      "NULL",
		},
		dateLayouts: []string{},
	}

	plan := NewTargetCastPlan(cols, sp.dateLayouts, sp)

	// Empty string in string column with EmptyAsNull=false → preserved
	row := []any{"", "123", "1.5"}
	row = plan.CastRow(row)
	assert.Equal(t, "", row[0], "empty string should be preserved when EmptyAsNull=false")
	assert.Equal(t, int64(123), row[1])
	assert.Equal(t, 1.5, row[2])

	// Empty string in non-string column → nil
	row = []any{"hello", "", ""}
	row = plan.CastRow(row)
	assert.Equal(t, "hello", row[0])
	assert.Nil(t, row[1], "empty non-string should become nil")
	assert.Nil(t, row[2], "empty non-string should become nil")

	// NULL sentinel
	row = []any{"NULL", "NULL", "NULL"}
	row = plan.CastRow(row)
	assert.Nil(t, row[0], "NULL sentinel should become nil")
	assert.Nil(t, row[1])
	assert.Nil(t, row[2])

	// With EmptyAsNull=true
	sp2 := &StreamProcessor{
		Config: &StreamConfig{
			EmptyAsNull: true,
			NullIf:      "NULL",
		},
		dateLayouts: []string{},
	}
	plan2 := NewTargetCastPlan(cols, sp2.dateLayouts, sp2)
	row = []any{"", "123", "1.5"}
	row = plan2.CastRow(row)
	assert.Nil(t, row[0], "empty string should become nil when EmptyAsNull=true")
}

func TestCastRowBlankCount(t *testing.T) {
	cols := Columns{
		{Name: "a", Type: StringType, DbType: "string"},
		{Name: "b", Type: StringType, DbType: "string"},
		{Name: "c", Type: BigIntType, DbType: "int64"},
	}
	sp := &StreamProcessor{
		Config: &StreamConfig{
			EmptyAsNull: true,
			NullIf:      "NULL",
		},
		dateLayouts: []string{},
	}
	plan := NewTargetCastPlan(cols, sp.dateLayouts, sp)

	// All blank
	row := []any{"", "", ""}
	plan.CastRow(row)
	assert.Equal(t, 3, plan.lastBlankCount)

	// NULL sentinel should NOT count as blank (matches generic CastVal)
	row = []any{"NULL", "NULL", "NULL"}
	plan.CastRow(row)
	assert.Equal(t, 0, plan.lastBlankCount, "null_if matches should not count as blank")

	// Mix
	row = []any{"hello", "", "42"}
	plan.CastRow(row)
	assert.Equal(t, 1, plan.lastBlankCount)
}

func TestCastRowDatetime(t *testing.T) {
	cols := Columns{
		{Name: "ts", Type: DatetimeType, DbType: "datetime64(3, 'UTC')"},
	}
	layouts := []string{
		"2006-01-02 15:04:05.000000 +00",
		"2006-01-02 15:04:05.000000 -07",
		"2006-01-02 15:04:05",
	}
	sp := &StreamProcessor{
		Config:      &StreamConfig{NullIf: "NULL"},
		dateLayouts: layouts,
	}
	plan := NewTargetCastPlan(cols, layouts, sp)

	row := []any{"2025-01-06 13:09:20.000000 +00"}
	row = plan.CastRow(row)
	ts, ok := row[0].(time.Time)
	assert.True(t, ok, "should parse to time.Time")
	assert.Equal(t, 2025, ts.Year())
	assert.Equal(t, time.January, ts.Month())
	assert.Equal(t, 6, ts.Day())

	// Per-column cache should now be set
	assert.NotEmpty(t, plan.layoutCaches[0])

	// Second row uses cache
	row = []any{"2025-06-15 12:00:00.000000 +00"}
	row = plan.CastRow(row)
	ts2, ok := row[0].(time.Time)
	assert.True(t, ok)
	assert.Equal(t, 2025, ts2.Year())
	assert.Equal(t, time.June, ts2.Month())

	// Date-only values (midnight UTC) should be normalized to UTC,
	// matching generic ParseTime behavior.
	dateCols := Columns{
		{Name: "d", Type: DatetimeType, DbType: "date"},
	}
	dateLayouts := []string{"2006-01-02"}
	datePlan := NewTargetCastPlan(dateCols, dateLayouts, sp)
	row = []any{"2025-03-15"}
	row = datePlan.CastRow(row)
	dt, ok := row[0].(time.Time)
	assert.True(t, ok, "date should parse to time.Time")
	assert.True(t, dt.Location() == time.UTC, "date-only value should be in UTC")
	assert.Equal(t, 15, dt.Day())
}

func TestParserOverflow(t *testing.T) {
	// int8 overflow: "999" should error, not silently truncate
	_, err := parseInt8("999")
	assert.Error(t, err, "int8 should reject 999")

	// "128.0" via float fallback should error for int8 (max 127)
	_, err = parseInt8("128.0")
	assert.Error(t, err, "int8 should reject 128.0")

	// "-1" in uint8 should error
	_, err = parseUint8("-1")
	assert.Error(t, err, "uint8 should reject -1")

	// "256.0" in uint8 should error
	_, err = parseUint8("256.0")
	assert.Error(t, err, "uint8 should reject 256.0")

	// Valid float-formatted values within range should work
	v, err := parseInt8("42.0")
	assert.NoError(t, err)
	assert.Equal(t, int8(42), v)

	v, err = parseUint16("100.0")
	assert.NoError(t, err)
	assert.Equal(t, uint16(100), v)

	// Fractional values should be rejected (not silently truncated)
	_, err = parseInt8("42.5")
	assert.Error(t, err, "int8 should reject fractional 42.5")

	_, err = parseUint8("255.9")
	assert.Error(t, err, "uint8 should reject fractional 255.9")

	// int64/uint64 have no float fallback at all — decimal-formatted values error
	_, err = parseInt64("100.7")
	assert.Error(t, err, "int64 should reject float-formatted input")
	_, err = parseUint64("42.0")
	assert.Error(t, err, "uint64 should reject float-formatted input")

	// Large uint64 values that fit in integer parse should still work
	v, err = parseUint64("18446744073709551615")
	assert.NoError(t, err)
	assert.Equal(t, uint64(18446744073709551615), v)

	// Regression: max-range decimal-formatted values must not silently corrupt.
	// 9223372036854775807.0 as float64 rounds to 9.223372036854776e+18,
	// then int64() wraps to -9223372036854775808.
	_, err = parseInt64("9223372036854775807.0")
	assert.Error(t, err, "int64 should reject 9223372036854775807.0")

	// 18446744073709551615.0 as float64→uint64 gives 9223372036854775808
	_, err = parseUint64("18446744073709551615.0")
	assert.Error(t, err, "uint64 should reject 18446744073709551615.0")
}

func TestStreamProcessorFastCastGuard(t *testing.T) {
	sp := &StreamProcessor{
		Config:      &StreamConfig{NullIf: "NULL"},
		dateLayouts: []string{},
	}

	// Fast cast plan is not set by default
	assert.False(t, sp.HasTargetCastPlan(), "fast cast should be disabled by default")

	// After setting, it should be active
	cols := Columns{
		{Name: "a", Type: BigIntType, DbType: "int64"},
		{Name: "b", Type: StringType, DbType: "string"},
	}
	sp.SetTargetCastPlan(cols)
	assert.True(t, sp.HasTargetCastPlan(), "fast cast should be enabled after SetTargetCastPlan")

	// CastRowToTarget should work and update rowBlankValCnt
	row := sp.CastRowToTarget([]any{"42", ""})
	assert.Equal(t, int64(42), row[0])
	assert.Equal(t, "", row[1]) // string col with EmptyAsNull=false → preserved
}

func TestApplyTargetCastPlanExistingStreams(t *testing.T) {
	// Verify that ApplyTargetCastPlan sets the plan on streams
	// that already exist in the dataflow (regression test for the case
	// where streams are created before writeDirectly runs).
	df := NewDataflow()
	cols := Columns{
		{Name: "id", Type: BigIntType, DbType: "int64"},
		{Name: "name", Type: StringType, DbType: "string"},
	}

	// Simulate streams already pushed before fast cast is configured
	ds1 := NewDatastream(cols)
	ds2 := NewDatastream(cols)
	df.Streams = append(df.Streams, ds1, ds2)

	// Neither stream should have a plan yet
	assert.False(t, ds1.Sp.HasTargetCastPlan())
	assert.False(t, ds2.Sp.HasTargetCastPlan())

	// Apply plan — should set on both existing streams and on dataflow
	df.ApplyTargetCastPlan(cols)

	assert.True(t, ds1.Sp.HasTargetCastPlan(), "existing stream 1 should get plan")
	assert.True(t, ds2.Sp.HasTargetCastPlan(), "existing stream 2 should get plan")
	assert.Equal(t, len(cols), len(df.TargetCastColumns), "dataflow should store columns")

	// Verify the plan actually works on existing streams
	row := ds1.Sp.CastRowToTarget([]any{"42", "hello"})
	assert.Equal(t, int64(42), row[0])
	assert.Equal(t, "hello", row[1])
}

func TestCastRowPadding(t *testing.T) {
	cols := Columns{
		{Name: "a", Type: BigIntType, DbType: "int64"},
		{Name: "b", Type: StringType, DbType: "string"},
		{Name: "c", Type: FloatType, DbType: "float64"},
	}
	sp := &StreamProcessor{
		Config:      &StreamConfig{NullIf: "NULL"},
		dateLayouts: []string{},
	}
	plan := NewTargetCastPlan(cols, sp.dateLayouts, sp)

	// Row shorter than target schema → padded with nil
	row := []any{"42"}
	row = plan.CastRow(row)
	assert.Len(t, row, 3)
	assert.Equal(t, int64(42), row[0])
	assert.Nil(t, row[1])
	assert.Nil(t, row[2])
}
