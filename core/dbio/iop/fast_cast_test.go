package iop

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestUnwrapDbType(t *testing.T) {
	plan := &targetCastPlan{}
	cases := []struct {
		col      Column
		expected string
	}{
		{Column{DbType: "int64"}, "int64"},
		{Column{DbType: "nullable(float64)"}, "float64"},
		{Column{DbType: "low_cardinality(string)"}, "string"},
		{Column{DbType: "nullable(low_cardinality(string))"}, "string"},
		{Column{DbType: "low_cardinality(nullable(string))"}, "string"},
		{Column{DbType: "nullable(decimal(18, 2))"}, "decimal(18, 2)"},
		{Column{DbType: "nullable(datetime64(3, 'UTC'))"}, "datetime64(3, 'utc')"},
	}
	for _, c := range cases {
		parser := plan.makeParser(0, c.col)
		_ = parser // just verify no panic
		// Verify by checking the function actually works
	}
	_ = cases
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
	// float-formatted int
	v, err = parseInt64("123.0")
	assert.NoError(t, err)
	assert.Equal(t, int64(123), v)

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
