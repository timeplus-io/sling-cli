package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// colKind identifies the Go type expected in the columnar buffer for a Proton column.
type colKind int

const (
	colKindString colKind = iota
	colKindBool
	colKindInt8
	colKindInt16
	colKindInt32
	colKindInt64
	colKindUint8
	colKindUint16
	colKindUint32
	colKindUint64
	colKindFloat32
	colKindFloat64
	colKindDatetime // time.Time
	colKindDecimal  // decimal.Decimal
	colKindUnknown
)

// colBuffer is a per-column typed slice buffer.
// It accumulates values from CastRowToTarget (already typed) and exposes
// them as a typed slice for batch.Column(i).Append().
type colBuffer struct {
	kind     colKind
	nullable bool

	// Exactly one of these is active, depending on kind + nullable.
	// Non-nullable value slices:
	sVals   []string
	bVals   []bool
	i8Vals  []int8
	i16Vals []int16
	i32Vals []int32
	i64Vals []int64
	u8Vals  []uint8
	u16Vals []uint16
	u32Vals []uint32
	u64Vals []uint64
	f32Vals []float32
	f64Vals []float64
	tVals   []time.Time
	dVals   []decimal.Decimal

	// Nullable pointer slices:
	spVals   []*string
	bpVals   []*bool
	i8pVals  []*int8
	i16pVals []*int16
	i32pVals []*int32
	i64pVals []*int64
	u8pVals  []*uint8
	u16pVals []*uint16
	u32pVals []*uint32
	u64pVals []*uint64
	f32pVals []*float32
	f64pVals []*float64
	tpVals   []*time.Time
	dpVals   []*decimal.Decimal
}

// columnarBuffer accumulates rows into per-column typed slices
// for use with the proton-go-driver columnar Append API.
type columnarBuffer struct {
	cols    []colBuffer
	size    int
	numCols int
}

// peelWrappers strips nullable(...) and low_cardinality(...) from a Proton DbType,
// returning the inner type and whether it was wrapped in nullable.
func peelWrappers(dbType string) (inner string, isNullable bool) {
	dbType = strings.ToLower(dbType)
	for {
		if strings.HasPrefix(dbType, "nullable(") && strings.HasSuffix(dbType, ")") {
			dbType = dbType[len("nullable(") : len(dbType)-1]
			isNullable = true
		} else if strings.HasPrefix(dbType, "low_cardinality(") && strings.HasSuffix(dbType, ")") {
			dbType = dbType[len("low_cardinality(") : len(dbType)-1]
		} else {
			break
		}
	}
	return dbType, isNullable
}

// resolveColKind maps a peeled Proton DbType to a colKind.
func resolveColKind(dbType string, col iop.Column) colKind {
	switch dbType {
	case "bool":
		return colKindBool
	case "int8":
		return colKindInt8
	case "int16":
		return colKindInt16
	case "int32":
		return colKindInt32
	case "int64":
		return colKindInt64
	case "uint8":
		return colKindUint8
	case "uint16":
		return colKindUint16
	case "uint32":
		return colKindUint32
	case "uint64":
		return colKindUint64
	case "float32":
		return colKindFloat32
	case "float64":
		return colKindFloat64
	case "string":
		return colKindString
	}

	if strings.HasPrefix(dbType, "datetime") || dbType == "date" || dbType == "date32" {
		return colKindDatetime
	}
	if strings.HasPrefix(dbType, "decimal") {
		return colKindDecimal
	}

	// Fallback via sling ColumnType
	switch {
	case col.Type == iop.BoolType:
		return colKindBool
	case col.Type == iop.SmallIntType:
		return colKindInt32
	case col.Type == iop.IntegerType || col.Type == iop.BigIntType:
		return colKindInt64
	case col.Type == iop.FloatType:
		return colKindFloat64
	case col.Type == iop.DecimalType:
		return colKindDecimal
	case col.Type.IsDatetime() || col.Type.IsDate():
		return colKindDatetime
	case col.Type == iop.StringType || col.Type == iop.TextType:
		return colKindString
	default:
		return colKindUnknown
	}
}

// CanUseColumnarFastPath returns true if all columns are scalar types
// supported by the columnar buffer (no array, map, tuple, etc.).
func CanUseColumnarFastPath(cols iop.Columns) bool {
	for _, col := range cols {
		inner, _ := peelWrappers(col.DbType)
		if resolveColKind(inner, col) == colKindUnknown {
			return false
		}
		// Reject array/map/tuple types explicitly
		if strings.HasPrefix(inner, "array(") ||
			strings.HasPrefix(inner, "map(") ||
			strings.HasPrefix(inner, "tuple(") {
			return false
		}
	}
	return true
}

// newColumnarBuffer creates a buffer for the given columns with pre-allocated capacity.
func newColumnarBuffer(insFields iop.Columns, capacity int) *columnarBuffer {
	cb := &columnarBuffer{
		cols:    make([]colBuffer, len(insFields)),
		numCols: len(insFields),
	}
	for i, col := range insFields {
		inner, isNullable := peelWrappers(col.DbType)
		kind := resolveColKind(inner, col)
		cb.cols[i] = colBuffer{
			kind:     kind,
			nullable: isNullable,
		}
		cb.cols[i].allocate(capacity)
	}
	return cb
}

// allocate pre-allocates the typed slice for this column.
func (c *colBuffer) allocate(capacity int) {
	if c.nullable {
		switch c.kind {
		case colKindString:
			c.spVals = make([]*string, 0, capacity)
		case colKindBool:
			c.bpVals = make([]*bool, 0, capacity)
		case colKindInt8:
			c.i8pVals = make([]*int8, 0, capacity)
		case colKindInt16:
			c.i16pVals = make([]*int16, 0, capacity)
		case colKindInt32:
			c.i32pVals = make([]*int32, 0, capacity)
		case colKindInt64:
			c.i64pVals = make([]*int64, 0, capacity)
		case colKindUint8:
			c.u8pVals = make([]*uint8, 0, capacity)
		case colKindUint16:
			c.u16pVals = make([]*uint16, 0, capacity)
		case colKindUint32:
			c.u32pVals = make([]*uint32, 0, capacity)
		case colKindUint64:
			c.u64pVals = make([]*uint64, 0, capacity)
		case colKindFloat32:
			c.f32pVals = make([]*float32, 0, capacity)
		case colKindFloat64:
			c.f64pVals = make([]*float64, 0, capacity)
		case colKindDatetime:
			c.tpVals = make([]*time.Time, 0, capacity)
		case colKindDecimal:
			c.dpVals = make([]*decimal.Decimal, 0, capacity)
		}
	} else {
		switch c.kind {
		case colKindString:
			c.sVals = make([]string, 0, capacity)
		case colKindBool:
			c.bVals = make([]bool, 0, capacity)
		case colKindInt8:
			c.i8Vals = make([]int8, 0, capacity)
		case colKindInt16:
			c.i16Vals = make([]int16, 0, capacity)
		case colKindInt32:
			c.i32Vals = make([]int32, 0, capacity)
		case colKindInt64:
			c.i64Vals = make([]int64, 0, capacity)
		case colKindUint8:
			c.u8Vals = make([]uint8, 0, capacity)
		case colKindUint16:
			c.u16Vals = make([]uint16, 0, capacity)
		case colKindUint32:
			c.u32Vals = make([]uint32, 0, capacity)
		case colKindUint64:
			c.u64Vals = make([]uint64, 0, capacity)
		case colKindFloat32:
			c.f32Vals = make([]float32, 0, capacity)
		case colKindFloat64:
			c.f64Vals = make([]float64, 0, capacity)
		case colKindDatetime:
			c.tVals = make([]time.Time, 0, capacity)
		case colKindDecimal:
			c.dVals = make([]decimal.Decimal, 0, capacity)
		}
	}
}

// appendVal appends a single value (already typed by CastRowToTarget) to this column buffer.
// For nullable columns, nil becomes a nil pointer. For non-nullable columns, nil becomes the zero value.
func (c *colBuffer) appendVal(val any) error {
	if c.nullable {
		return c.appendNullable(val)
	}
	return c.appendNonNull(val)
}

// appendNonNull appends a typed value to a non-nullable column.
// nil is mapped to the zero value. If the value is still a string
// (CastRowToTarget not yet applied or parse error fallback), it is
// converted using the appropriate strconv/cast function.
func (c *colBuffer) appendNonNull(val any) error {
	if val == nil {
		// Non-nullable columns get zero value for nil
		switch c.kind {
		case colKindString:
			c.sVals = append(c.sVals, "")
		case colKindBool:
			c.bVals = append(c.bVals, false)
		case colKindInt8:
			c.i8Vals = append(c.i8Vals, 0)
		case colKindInt16:
			c.i16Vals = append(c.i16Vals, 0)
		case colKindInt32:
			c.i32Vals = append(c.i32Vals, 0)
		case colKindInt64:
			c.i64Vals = append(c.i64Vals, 0)
		case colKindUint8:
			c.u8Vals = append(c.u8Vals, 0)
		case colKindUint16:
			c.u16Vals = append(c.u16Vals, 0)
		case colKindUint32:
			c.u32Vals = append(c.u32Vals, 0)
		case colKindUint64:
			c.u64Vals = append(c.u64Vals, 0)
		case colKindFloat32:
			c.f32Vals = append(c.f32Vals, 0)
		case colKindFloat64:
			c.f64Vals = append(c.f64Vals, 0)
		case colKindDatetime:
			c.tVals = append(c.tVals, time.Time{})
		case colKindDecimal:
			c.dVals = append(c.dVals, decimal.Decimal{})
		}
		return nil
	}

	// Fallback conversions use cast.ToXxxE (error-returning variants) to match
	// processBatch semantics: invalid values like "abc" → int64 must fail the
	// batch, not silently coerce to zero.
	switch c.kind {
	case colKindString:
		v, err := cast.ToStringE(val)
		if err != nil {
			return fmt.Errorf("columnar: cannot convert %T to string: %w", val, err)
		}
		c.sVals = append(c.sVals, v)

	case colKindBool:
		if v, ok := val.(bool); ok {
			c.bVals = append(c.bVals, v)
		} else {
			v, err := cast.ToBoolE(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to bool: %w", val, err)
			}
			c.bVals = append(c.bVals, v)
		}

	case colKindInt8:
		if v, ok := val.(int8); ok {
			c.i8Vals = append(c.i8Vals, v)
		} else {
			v, err := cast.ToInt8E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int8: %w", val, err)
			}
			c.i8Vals = append(c.i8Vals, v)
		}

	case colKindInt16:
		if v, ok := val.(int16); ok {
			c.i16Vals = append(c.i16Vals, v)
		} else {
			v, err := cast.ToInt16E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int16: %w", val, err)
			}
			c.i16Vals = append(c.i16Vals, v)
		}

	case colKindInt32:
		if v, ok := val.(int32); ok {
			c.i32Vals = append(c.i32Vals, v)
		} else {
			v, err := cast.ToInt32E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int32: %w", val, err)
			}
			c.i32Vals = append(c.i32Vals, v)
		}

	case colKindInt64:
		if v, ok := val.(int64); ok {
			c.i64Vals = append(c.i64Vals, v)
		} else {
			v, err := cast.ToInt64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int64: %w", val, err)
			}
			c.i64Vals = append(c.i64Vals, v)
		}

	case colKindUint8:
		if v, ok := val.(uint8); ok {
			c.u8Vals = append(c.u8Vals, v)
		} else {
			v, err := cast.ToUint8E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint8: %w", val, err)
			}
			c.u8Vals = append(c.u8Vals, v)
		}

	case colKindUint16:
		if v, ok := val.(uint16); ok {
			c.u16Vals = append(c.u16Vals, v)
		} else {
			v, err := cast.ToUint16E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint16: %w", val, err)
			}
			c.u16Vals = append(c.u16Vals, v)
		}

	case colKindUint32:
		if v, ok := val.(uint32); ok {
			c.u32Vals = append(c.u32Vals, v)
		} else {
			v, err := cast.ToUint32E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint32: %w", val, err)
			}
			c.u32Vals = append(c.u32Vals, v)
		}

	case colKindUint64:
		if v, ok := val.(uint64); ok {
			c.u64Vals = append(c.u64Vals, v)
		} else {
			v, err := cast.ToUint64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint64: %w", val, err)
			}
			c.u64Vals = append(c.u64Vals, v)
		}

	case colKindFloat32:
		if v, ok := val.(float32); ok {
			c.f32Vals = append(c.f32Vals, v)
		} else if v, ok := val.(float64); ok {
			c.f32Vals = append(c.f32Vals, float32(v))
		} else {
			v, err := cast.ToFloat32E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to float32: %w", val, err)
			}
			c.f32Vals = append(c.f32Vals, v)
		}

	case colKindFloat64:
		if v, ok := val.(float64); ok {
			c.f64Vals = append(c.f64Vals, v)
		} else {
			v, err := cast.ToFloat64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to float64: %w", val, err)
			}
			c.f64Vals = append(c.f64Vals, v)
		}

	case colKindDatetime:
		if v, ok := val.(time.Time); ok {
			c.tVals = append(c.tVals, v)
		} else if s, ok := val.(string); ok {
			// Fallback: parse datetime string
			t, err := parseTimeString(s)
			if err != nil {
				return fmt.Errorf("columnar: cannot parse datetime %q: %w", s, err)
			}
			c.tVals = append(c.tVals, t)
		} else {
			return fmt.Errorf("columnar: expected time.Time, got %T", val)
		}

	case colKindDecimal:
		if v, ok := val.(decimal.Decimal); ok {
			c.dVals = append(c.dVals, v)
		} else if s, ok := val.(string); ok {
			v, err := decimal.NewFromString(s)
			if err != nil {
				return fmt.Errorf("columnar: cannot parse decimal %q: %w", s, err)
			}
			c.dVals = append(c.dVals, v)
		} else {
			v, err := cast.ToFloat64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to decimal: %w", val, err)
			}
			c.dVals = append(c.dVals, decimal.NewFromFloat(v))
		}

	default:
		return fmt.Errorf("columnar: unsupported colKind %d", c.kind)
	}
	return nil
}

// appendNullable appends a typed value to a nullable column (pointer slices).
// nil becomes a nil pointer. String fallback parsing is used when the value
// hasn't been cast yet.
func (c *colBuffer) appendNullable(val any) error {
	if val == nil {
		switch c.kind {
		case colKindString:
			c.spVals = append(c.spVals, nil)
		case colKindBool:
			c.bpVals = append(c.bpVals, nil)
		case colKindInt8:
			c.i8pVals = append(c.i8pVals, nil)
		case colKindInt16:
			c.i16pVals = append(c.i16pVals, nil)
		case colKindInt32:
			c.i32pVals = append(c.i32pVals, nil)
		case colKindInt64:
			c.i64pVals = append(c.i64pVals, nil)
		case colKindUint8:
			c.u8pVals = append(c.u8pVals, nil)
		case colKindUint16:
			c.u16pVals = append(c.u16pVals, nil)
		case colKindUint32:
			c.u32pVals = append(c.u32pVals, nil)
		case colKindUint64:
			c.u64pVals = append(c.u64pVals, nil)
		case colKindFloat32:
			c.f32pVals = append(c.f32pVals, nil)
		case colKindFloat64:
			c.f64pVals = append(c.f64pVals, nil)
		case colKindDatetime:
			c.tpVals = append(c.tpVals, nil)
		case colKindDecimal:
			c.dpVals = append(c.dpVals, nil)
		}
		return nil
	}

	// Fallback conversions use cast.ToXxxE (error-returning variants) to match
	// processBatch semantics: invalid values must fail, not silently coerce to zero.
	switch c.kind {
	case colKindString:
		v, err := cast.ToStringE(val)
		if err != nil {
			return fmt.Errorf("columnar: cannot convert %T to string: %w", val, err)
		}
		c.spVals = append(c.spVals, &v)

	case colKindBool:
		if v, ok := val.(bool); ok {
			c.bpVals = append(c.bpVals, &v)
		} else {
			v, err := cast.ToBoolE(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to bool: %w", val, err)
			}
			c.bpVals = append(c.bpVals, &v)
		}

	case colKindInt8:
		if v, ok := val.(int8); ok {
			c.i8pVals = append(c.i8pVals, &v)
		} else {
			v, err := cast.ToInt8E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int8: %w", val, err)
			}
			c.i8pVals = append(c.i8pVals, &v)
		}

	case colKindInt16:
		if v, ok := val.(int16); ok {
			c.i16pVals = append(c.i16pVals, &v)
		} else {
			v, err := cast.ToInt16E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int16: %w", val, err)
			}
			c.i16pVals = append(c.i16pVals, &v)
		}

	case colKindInt32:
		if v, ok := val.(int32); ok {
			c.i32pVals = append(c.i32pVals, &v)
		} else {
			v, err := cast.ToInt32E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int32: %w", val, err)
			}
			c.i32pVals = append(c.i32pVals, &v)
		}

	case colKindInt64:
		if v, ok := val.(int64); ok {
			c.i64pVals = append(c.i64pVals, &v)
		} else {
			v, err := cast.ToInt64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to int64: %w", val, err)
			}
			c.i64pVals = append(c.i64pVals, &v)
		}

	case colKindUint8:
		if v, ok := val.(uint8); ok {
			c.u8pVals = append(c.u8pVals, &v)
		} else {
			v, err := cast.ToUint8E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint8: %w", val, err)
			}
			c.u8pVals = append(c.u8pVals, &v)
		}

	case colKindUint16:
		if v, ok := val.(uint16); ok {
			c.u16pVals = append(c.u16pVals, &v)
		} else {
			v, err := cast.ToUint16E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint16: %w", val, err)
			}
			c.u16pVals = append(c.u16pVals, &v)
		}

	case colKindUint32:
		if v, ok := val.(uint32); ok {
			c.u32pVals = append(c.u32pVals, &v)
		} else {
			v, err := cast.ToUint32E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint32: %w", val, err)
			}
			c.u32pVals = append(c.u32pVals, &v)
		}

	case colKindUint64:
		if v, ok := val.(uint64); ok {
			c.u64pVals = append(c.u64pVals, &v)
		} else {
			v, err := cast.ToUint64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to uint64: %w", val, err)
			}
			c.u64pVals = append(c.u64pVals, &v)
		}

	case colKindFloat32:
		if v, ok := val.(float32); ok {
			c.f32pVals = append(c.f32pVals, &v)
		} else if f64, ok := val.(float64); ok {
			v := float32(f64)
			c.f32pVals = append(c.f32pVals, &v)
		} else {
			v, err := cast.ToFloat32E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to float32: %w", val, err)
			}
			c.f32pVals = append(c.f32pVals, &v)
		}

	case colKindFloat64:
		if v, ok := val.(float64); ok {
			c.f64pVals = append(c.f64pVals, &v)
		} else {
			v, err := cast.ToFloat64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to float64: %w", val, err)
			}
			c.f64pVals = append(c.f64pVals, &v)
		}

	case colKindDatetime:
		if v, ok := val.(time.Time); ok {
			c.tpVals = append(c.tpVals, &v)
		} else if s, ok := val.(string); ok {
			t, err := parseTimeString(s)
			if err != nil {
				return fmt.Errorf("columnar: cannot parse datetime %q: %w", s, err)
			}
			c.tpVals = append(c.tpVals, &t)
		} else {
			return fmt.Errorf("columnar: expected time.Time, got %T", val)
		}

	case colKindDecimal:
		if v, ok := val.(decimal.Decimal); ok {
			c.dpVals = append(c.dpVals, &v)
		} else if s, ok := val.(string); ok {
			v, err := decimal.NewFromString(s)
			if err != nil {
				return fmt.Errorf("columnar: cannot parse decimal %q: %w", s, err)
			}
			c.dpVals = append(c.dpVals, &v)
		} else {
			v, err := cast.ToFloat64E(val)
			if err != nil {
				return fmt.Errorf("columnar: cannot convert %T to decimal: %w", val, err)
			}
			d := decimal.NewFromFloat(v)
			c.dpVals = append(c.dpVals, &d)
		}

	default:
		return fmt.Errorf("columnar: unsupported colKind %d for nullable", c.kind)
	}
	return nil
}

// parseTimeString is a fallback datetime parser for values that CastRowToTarget
// didn't convert (e.g., rows buffered before the fast-cast plan was applied).
func parseTimeString(s string) (time.Time, error) {
	// Try common formats used by sling CSV exports
	for _, layout := range []string{
		"2006-01-02 15:04:05.000000 -07",
		"2006-01-02 15:04:05.000000 +00",
		"2006-01-02 15:04:05.000 -07",
		"2006-01-02 15:04:05 -07",
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	// Last resort: try Unix timestamp
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(ts, 0), nil
	}
	return time.Time{}, fmt.Errorf("unrecognized datetime format")
}

// typedSlice returns the accumulated typed slice for use with batch.Column(i).Append().
func (c *colBuffer) typedSlice() any {
	if c.nullable {
		switch c.kind {
		case colKindString:
			return c.spVals
		case colKindBool:
			return c.bpVals
		case colKindInt8:
			return c.i8pVals
		case colKindInt16:
			return c.i16pVals
		case colKindInt32:
			return c.i32pVals
		case colKindInt64:
			return c.i64pVals
		case colKindUint8:
			return c.u8pVals
		case colKindUint16:
			return c.u16pVals
		case colKindUint32:
			return c.u32pVals
		case colKindUint64:
			return c.u64pVals
		case colKindFloat32:
			return c.f32pVals
		case colKindFloat64:
			return c.f64pVals
		case colKindDatetime:
			return c.tpVals
		case colKindDecimal:
			return c.dpVals
		}
	}
	switch c.kind {
	case colKindString:
		return c.sVals
	case colKindBool:
		return c.bVals
	case colKindInt8:
		return c.i8Vals
	case colKindInt16:
		return c.i16Vals
	case colKindInt32:
		return c.i32Vals
	case colKindInt64:
		return c.i64Vals
	case colKindUint8:
		return c.u8Vals
	case colKindUint16:
		return c.u16Vals
	case colKindUint32:
		return c.u32Vals
	case colKindUint64:
		return c.u64Vals
	case colKindFloat32:
		return c.f32Vals
	case colKindFloat64:
		return c.f64Vals
	case colKindDatetime:
		return c.tVals
	case colKindDecimal:
		return c.dVals
	}
	return nil
}

// reset truncates all slices to zero length without deallocating the backing arrays.
func (c *colBuffer) reset() {
	if c.nullable {
		switch c.kind {
		case colKindString:
			c.spVals = c.spVals[:0]
		case colKindBool:
			c.bpVals = c.bpVals[:0]
		case colKindInt8:
			c.i8pVals = c.i8pVals[:0]
		case colKindInt16:
			c.i16pVals = c.i16pVals[:0]
		case colKindInt32:
			c.i32pVals = c.i32pVals[:0]
		case colKindInt64:
			c.i64pVals = c.i64pVals[:0]
		case colKindUint8:
			c.u8pVals = c.u8pVals[:0]
		case colKindUint16:
			c.u16pVals = c.u16pVals[:0]
		case colKindUint32:
			c.u32pVals = c.u32pVals[:0]
		case colKindUint64:
			c.u64pVals = c.u64pVals[:0]
		case colKindFloat32:
			c.f32pVals = c.f32pVals[:0]
		case colKindFloat64:
			c.f64pVals = c.f64pVals[:0]
		case colKindDatetime:
			c.tpVals = c.tpVals[:0]
		case colKindDecimal:
			c.dpVals = c.dpVals[:0]
		}
	} else {
		switch c.kind {
		case colKindString:
			c.sVals = c.sVals[:0]
		case colKindBool:
			c.bVals = c.bVals[:0]
		case colKindInt8:
			c.i8Vals = c.i8Vals[:0]
		case colKindInt16:
			c.i16Vals = c.i16Vals[:0]
		case colKindInt32:
			c.i32Vals = c.i32Vals[:0]
		case colKindInt64:
			c.i64Vals = c.i64Vals[:0]
		case colKindUint8:
			c.u8Vals = c.u8Vals[:0]
		case colKindUint16:
			c.u16Vals = c.u16Vals[:0]
		case colKindUint32:
			c.u32Vals = c.u32Vals[:0]
		case colKindUint64:
			c.u64Vals = c.u64Vals[:0]
		case colKindFloat32:
			c.f32Vals = c.f32Vals[:0]
		case colKindFloat64:
			c.f64Vals = c.f64Vals[:0]
		case colKindDatetime:
			c.tVals = c.tVals[:0]
		case colKindDecimal:
			c.dVals = c.dVals[:0]
		}
	}
}

// AppendRow appends a single row (already typed by CastRowToTarget) to the columnar buffer.
func (cb *columnarBuffer) AppendRow(row []any) error {
	for i := 0; i < cb.numCols && i < len(row); i++ {
		if err := cb.cols[i].appendVal(row[i]); err != nil {
			return g.Error(err, "columnar append failed for column %d", i)
		}
	}
	// If row is shorter than column count, pad with nil/zero
	for i := len(row); i < cb.numCols; i++ {
		if err := cb.cols[i].appendVal(nil); err != nil {
			return g.Error(err, "columnar append nil failed for column %d", i)
		}
	}
	cb.size++
	return nil
}

// Reset truncates all column buffers without deallocating.
func (cb *columnarBuffer) Reset() {
	for i := range cb.cols {
		cb.cols[i].reset()
	}
	cb.size = 0
}
