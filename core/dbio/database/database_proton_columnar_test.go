package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── peelWrappers ───────────────────────────────────────────────────────

func TestPeelWrappers(t *testing.T) {
	tests := []struct {
		input      string
		wantInner  string
		wantNullable bool
	}{
		{"int64", "int64", false},
		{"nullable(float64)", "float64", true},
		{"low_cardinality(string)", "string", false},
		{"nullable(low_cardinality(string))", "string", true},
		{"low_cardinality(nullable(string))", "string", true},
		{"Nullable(Float64)", "float64", true},          // case insensitive
		{"LOW_CARDINALITY(NULLABLE(INT32))", "int32", true},
		{"datetime64(3, 'UTC')", "datetime64(3, 'utc')", false},
		{"nullable(datetime64(3, 'UTC'))", "datetime64(3, 'utc')", true},
		{"nullable(decimal(18,4))", "decimal(18,4)", true},
		{"low_cardinality(nullable(decimal(18,2)))", "decimal(18,2)", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			inner, isNullable := peelWrappers(tt.input)
			assert.Equal(t, tt.wantInner, inner)
			assert.Equal(t, tt.wantNullable, isNullable)
		})
	}
}

// ─── resolveColKind ─────────────────────────────────────────────────────

func TestResolveColKind(t *testing.T) {
	tests := []struct {
		dbType string
		col    iop.Column
		want   colKind
	}{
		{"int64", iop.Column{}, colKindInt64},
		{"uint8", iop.Column{}, colKindUint8},
		{"float64", iop.Column{}, colKindFloat64},
		{"float32", iop.Column{}, colKindFloat32},
		{"string", iop.Column{}, colKindString},
		{"bool", iop.Column{}, colKindBool},
		{"datetime64(3, 'utc')", iop.Column{}, colKindDatetime},
		{"date", iop.Column{}, colKindDatetime},
		{"date32", iop.Column{}, colKindDatetime},
		{"decimal(18,4)", iop.Column{}, colKindDecimal},
		// Fallback via sling ColumnType
		{"custom_type", iop.Column{Type: iop.IntegerType}, colKindInt64},
		{"custom_type", iop.Column{Type: iop.FloatType}, colKindFloat64},
		{"custom_type", iop.Column{Type: iop.StringType}, colKindString},
		{"custom_type", iop.Column{Type: iop.BoolType}, colKindBool},
		// Unknown
		{"array(int64)", iop.Column{}, colKindUnknown},
		{"map(string, int64)", iop.Column{}, colKindUnknown},
		{"tuple(string, int64)", iop.Column{}, colKindUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			got := resolveColKind(tt.dbType, tt.col)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ─── CanUseColumnarFastPath ─────────────────────────────────────────────

func TestCanUseColumnarFastPath(t *testing.T) {
	t.Run("all scalar", func(t *testing.T) {
		cols := iop.Columns{
			{Name: "id", DbType: "int64"},
			{Name: "name", DbType: "string"},
			{Name: "val", DbType: "nullable(float64)"},
			{Name: "ts", DbType: "datetime64(3, 'UTC')"},
		}
		assert.True(t, CanUseColumnarFastPath(cols))
	})

	t.Run("array column rejects", func(t *testing.T) {
		cols := iop.Columns{
			{Name: "id", DbType: "int64"},
			{Name: "tags", DbType: "array(string)"},
		}
		assert.False(t, CanUseColumnarFastPath(cols))
	})

	t.Run("map column rejects", func(t *testing.T) {
		cols := iop.Columns{
			{Name: "id", DbType: "int64"},
			{Name: "meta", DbType: "map(string, string)"},
		}
		assert.False(t, CanUseColumnarFastPath(cols))
	})

	t.Run("empty columns", func(t *testing.T) {
		assert.True(t, CanUseColumnarFastPath(iop.Columns{}))
	})
}

// ─── colBuffer: appendVal + typedSlice (non-nullable) ───────────────────

func TestColBuffer_NonNullable_Int64(t *testing.T) {
	c := colBuffer{kind: colKindInt64, nullable: false}
	c.allocate(4)

	// Direct typed value
	require.NoError(t, c.appendVal(int64(42)))
	// String fallback
	require.NoError(t, c.appendVal("123"))
	// nil → zero value
	require.NoError(t, c.appendVal(nil))

	slice := c.typedSlice().([]int64)
	assert.Equal(t, []int64{42, 123, 0}, slice)
}

func TestColBuffer_NonNullable_Float64(t *testing.T) {
	c := colBuffer{kind: colKindFloat64, nullable: false}
	c.allocate(4)

	require.NoError(t, c.appendVal(float64(3.14)))
	require.NoError(t, c.appendVal("2.71"))
	require.NoError(t, c.appendVal(nil))

	slice := c.typedSlice().([]float64)
	assert.Equal(t, []float64{3.14, 2.71, 0}, slice)
}

func TestColBuffer_NonNullable_String(t *testing.T) {
	c := colBuffer{kind: colKindString, nullable: false}
	c.allocate(4)

	require.NoError(t, c.appendVal("hello"))
	require.NoError(t, c.appendVal(nil))      // → ""
	require.NoError(t, c.appendVal(int64(42))) // cast.ToStringE

	slice := c.typedSlice().([]string)
	assert.Equal(t, []string{"hello", "", "42"}, slice)
}

func TestColBuffer_NonNullable_Bool(t *testing.T) {
	c := colBuffer{kind: colKindBool, nullable: false}
	c.allocate(4)

	require.NoError(t, c.appendVal(true))
	require.NoError(t, c.appendVal(false))
	require.NoError(t, c.appendVal(nil)) // → false

	slice := c.typedSlice().([]bool)
	assert.Equal(t, []bool{true, false, false}, slice)
}

func TestColBuffer_NonNullable_Datetime(t *testing.T) {
	c := colBuffer{kind: colKindDatetime, nullable: false}
	c.allocate(4)

	ts := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, c.appendVal(ts))
	// String fallback
	require.NoError(t, c.appendVal("2025-06-15 10:30:00"))
	require.NoError(t, c.appendVal(nil)) // → zero time

	slice := c.typedSlice().([]time.Time)
	assert.Equal(t, ts, slice[0])
	assert.Equal(t, time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC), slice[1])
	assert.True(t, slice[2].IsZero())
}

func TestColBuffer_NonNullable_Decimal(t *testing.T) {
	c := colBuffer{kind: colKindDecimal, nullable: false}
	c.allocate(4)

	d := decimal.NewFromFloat(123.456)
	require.NoError(t, c.appendVal(d))
	require.NoError(t, c.appendVal("789.01"))
	require.NoError(t, c.appendVal(float64(42.0)))
	require.NoError(t, c.appendVal(nil))

	slice := c.typedSlice().([]decimal.Decimal)
	assert.True(t, slice[0].Equal(d))
	assert.True(t, slice[1].Equal(decimal.RequireFromString("789.01")))
	assert.True(t, slice[2].Equal(decimal.NewFromFloat(42.0)))
	assert.True(t, slice[3].IsZero())
}

// ─── colBuffer: appendVal + typedSlice (nullable) ───────────────────────

func TestColBuffer_Nullable_Float64(t *testing.T) {
	c := colBuffer{kind: colKindFloat64, nullable: true}
	c.allocate(4)

	require.NoError(t, c.appendVal(float64(3.14)))
	require.NoError(t, c.appendVal(nil)) // → nil pointer
	require.NoError(t, c.appendVal("2.71"))

	slice := c.typedSlice().([]*float64)
	require.Len(t, slice, 3)
	assert.Equal(t, 3.14, *slice[0])
	assert.Nil(t, slice[1])
	assert.Equal(t, 2.71, *slice[2])
}

func TestColBuffer_Nullable_Int64(t *testing.T) {
	c := colBuffer{kind: colKindInt64, nullable: true}
	c.allocate(4)

	require.NoError(t, c.appendVal(int64(100)))
	require.NoError(t, c.appendVal(nil))
	require.NoError(t, c.appendVal(int64(-50)))

	slice := c.typedSlice().([]*int64)
	require.Len(t, slice, 3)
	assert.Equal(t, int64(100), *slice[0])
	assert.Nil(t, slice[1])
	assert.Equal(t, int64(-50), *slice[2])
}

func TestColBuffer_Nullable_String(t *testing.T) {
	c := colBuffer{kind: colKindString, nullable: true}
	c.allocate(4)

	require.NoError(t, c.appendVal("hello"))
	require.NoError(t, c.appendVal(nil))
	require.NoError(t, c.appendVal("world"))

	slice := c.typedSlice().([]*string)
	require.Len(t, slice, 3)
	assert.Equal(t, "hello", *slice[0])
	assert.Nil(t, slice[1])
	assert.Equal(t, "world", *slice[2])
}

func TestColBuffer_Nullable_Datetime(t *testing.T) {
	c := colBuffer{kind: colKindDatetime, nullable: true}
	c.allocate(4)

	ts := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	require.NoError(t, c.appendVal(ts))
	require.NoError(t, c.appendVal(nil))
	require.NoError(t, c.appendVal("2025-01-01"))

	slice := c.typedSlice().([]*time.Time)
	require.Len(t, slice, 3)
	assert.Equal(t, ts, *slice[0])
	assert.Nil(t, slice[1])
	assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), *slice[2])
}

// ─── Error cases ────────────────────────────────────────────────────────

func TestColBuffer_ErrorOnInvalidConversion(t *testing.T) {
	t.Run("string→int64 fails", func(t *testing.T) {
		c := colBuffer{kind: colKindInt64, nullable: false}
		c.allocate(1)
		err := c.appendVal("not_a_number")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "columnar:")
	})

	t.Run("string→float64 fails", func(t *testing.T) {
		c := colBuffer{kind: colKindFloat64, nullable: false}
		c.allocate(1)
		err := c.appendVal("abc")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "columnar:")
	})

	t.Run("string→bool fails", func(t *testing.T) {
		c := colBuffer{kind: colKindBool, nullable: false}
		c.allocate(1)
		err := c.appendVal("xyz")
		assert.Error(t, err)
	})

	t.Run("invalid datetime string", func(t *testing.T) {
		c := colBuffer{kind: colKindDatetime, nullable: false}
		c.allocate(1)
		err := c.appendVal("not-a-date")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "datetime")
	})

	t.Run("nullable string→int64 also fails", func(t *testing.T) {
		c := colBuffer{kind: colKindInt64, nullable: true}
		c.allocate(1)
		err := c.appendVal("abc")
		assert.Error(t, err)
	})

	t.Run("invalid decimal string", func(t *testing.T) {
		c := colBuffer{kind: colKindDecimal, nullable: false}
		c.allocate(1)
		err := c.appendVal("not_decimal")
		assert.Error(t, err)
	})
}

// ─── reset ──────────────────────────────────────────────────────────────

func TestColBuffer_Reset(t *testing.T) {
	c := colBuffer{kind: colKindFloat64, nullable: false}
	c.allocate(10)

	require.NoError(t, c.appendVal(float64(1.0)))
	require.NoError(t, c.appendVal(float64(2.0)))
	assert.Len(t, c.typedSlice().([]float64), 2)

	c.reset()
	assert.Len(t, c.typedSlice().([]float64), 0)
	// Capacity should be preserved
	assert.Equal(t, 10, cap(c.f64Vals))
}

func TestColBuffer_Reset_Nullable(t *testing.T) {
	c := colBuffer{kind: colKindInt64, nullable: true}
	c.allocate(10)

	require.NoError(t, c.appendVal(int64(1)))
	require.NoError(t, c.appendVal(nil))
	assert.Len(t, c.typedSlice().([]*int64), 2)

	c.reset()
	assert.Len(t, c.typedSlice().([]*int64), 0)
	assert.Equal(t, 10, cap(c.i64pVals))
}

// ─── columnarBuffer: AppendRow + Reset ──────────────────────────────────

func TestColumnarBuffer_AppendRow(t *testing.T) {
	cols := iop.Columns{
		{Name: "id", DbType: "int64"},
		{Name: "name", DbType: "string"},
		{Name: "val", DbType: "nullable(float64)"},
	}
	cb := newColumnarBuffer(cols, 10)

	require.NoError(t, cb.AppendRow([]any{int64(1), "alice", float64(3.14)}))
	require.NoError(t, cb.AppendRow([]any{int64(2), "bob", nil}))
	assert.Equal(t, 2, cb.size)

	// Verify column slices
	ids := cb.cols[0].typedSlice().([]int64)
	assert.Equal(t, []int64{1, 2}, ids)

	names := cb.cols[1].typedSlice().([]string)
	assert.Equal(t, []string{"alice", "bob"}, names)

	vals := cb.cols[2].typedSlice().([]*float64)
	require.Len(t, vals, 2)
	assert.Equal(t, 3.14, *vals[0])
	assert.Nil(t, vals[1])
}

func TestColumnarBuffer_AppendRow_ShortRow(t *testing.T) {
	cols := iop.Columns{
		{Name: "a", DbType: "int64"},
		{Name: "b", DbType: "string"},
		{Name: "c", DbType: "nullable(float64)"},
	}
	cb := newColumnarBuffer(cols, 5)

	// Row shorter than column count — missing columns padded with nil/zero
	require.NoError(t, cb.AppendRow([]any{int64(1)}))
	assert.Equal(t, 1, cb.size)

	ids := cb.cols[0].typedSlice().([]int64)
	assert.Equal(t, []int64{1}, ids)

	names := cb.cols[1].typedSlice().([]string)
	assert.Equal(t, []string{""}, names) // zero for non-nullable string

	vals := cb.cols[2].typedSlice().([]*float64)
	assert.Nil(t, vals[0]) // nil for nullable
}

func TestColumnarBuffer_Reset(t *testing.T) {
	cols := iop.Columns{
		{Name: "id", DbType: "int64"},
		{Name: "val", DbType: "nullable(float64)"},
	}
	cb := newColumnarBuffer(cols, 10)

	require.NoError(t, cb.AppendRow([]any{int64(1), float64(1.0)}))
	require.NoError(t, cb.AppendRow([]any{int64(2), nil}))
	assert.Equal(t, 2, cb.size)

	cb.Reset()
	assert.Equal(t, 0, cb.size)
	assert.Len(t, cb.cols[0].typedSlice().([]int64), 0)
	assert.Len(t, cb.cols[1].typedSlice().([]*float64), 0)

	// Can reuse after reset
	require.NoError(t, cb.AppendRow([]any{int64(3), float64(9.9)}))
	assert.Equal(t, 1, cb.size)
}

// ─── parseTimeString ────────────────────────────────────────────────────

func TestParseTimeString(t *testing.T) {
	tests := []struct {
		input    string
		wantUnix int64
	}{
		{"2025-01-01 12:00:00.000000 +00", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).Unix()},
		{"2025-01-01 12:00:00.000 +00", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).Unix()},
		{"2025-01-01 12:00:00 +00", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).Unix()},
		{"2025-01-01T12:00:00Z", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).Unix()},
		{"2025-01-01T12:00:00.000000Z", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).Unix()},
		{"2025-01-01 12:00:00", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC).Unix()},
		{"2025-01-01", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Unix()},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseTimeString(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.wantUnix, got.Unix())
		})
	}

	t.Run("invalid string", func(t *testing.T) {
		_, err := parseTimeString("not-a-date")
		assert.Error(t, err)
	})

	t.Run("unix timestamp", func(t *testing.T) {
		got, err := parseTimeString("1735689600")
		require.NoError(t, err)
		assert.Equal(t, time.Unix(1735689600, 0), got)
	})
}

// ─── Float32 overflow (consistent with processBatch) ────────────────────

func TestColBuffer_Float32_FromFloat64(t *testing.T) {
	c := colBuffer{kind: colKindFloat32, nullable: false}
	c.allocate(2)

	require.NoError(t, c.appendVal(float64(1.5)))
	slice := c.typedSlice().([]float32)
	assert.Equal(t, float32(1.5), slice[0])
}

func TestColBuffer_Float32_Nullable_FromFloat64(t *testing.T) {
	c := colBuffer{kind: colKindFloat32, nullable: true}
	c.allocate(3)

	require.NoError(t, c.appendVal(float64(2.5)))
	require.NoError(t, c.appendVal(nil))

	slice := c.typedSlice().([]*float32)
	require.Len(t, slice, 2)
	assert.Equal(t, float32(2.5), *slice[0])
	assert.Nil(t, slice[1])
}

// ─── All integer width types ────────────────────────────────────────────

func TestColBuffer_AllIntWidths(t *testing.T) {
	kinds := []struct {
		kind colKind
		val  any
	}{
		{colKindInt8, int8(42)},
		{colKindInt16, int16(1000)},
		{colKindInt32, int32(100000)},
		{colKindUint8, uint8(200)},
		{colKindUint16, uint16(60000)},
		{colKindUint32, uint32(3000000000)},
		{colKindUint64, uint64(18000000000000000000)},
	}
	for _, tt := range kinds {
		c := colBuffer{kind: tt.kind, nullable: false}
		c.allocate(2)
		require.NoError(t, c.appendVal(tt.val))
		require.NoError(t, c.appendVal(nil)) // zero
		assert.NotNil(t, c.typedSlice())
	}
}
