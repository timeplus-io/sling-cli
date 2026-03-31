package iop

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

// columnParser is a precompiled per-column parse function.
// It converts a raw CSV string value to the Go type expected by the target DB.
type columnParser func(val string) (any, error)

// targetCastPlan holds precompiled parsers for schema-driven fast casting.
// When a target table schema is known upfront, this eliminates per-cell
// type inference, stats collection, and transform lookup.
//
// NOT goroutine-safe: each Datastream must have its own plan instance
// because datetime layout caches are written during CastRow.
// PushStreamChan creates a new plan per stream via SetTargetCastPlan.
type targetCastPlan struct {
	parsers      []columnParser
	columns      Columns
	dateLayouts  []string // fallback layouts for datetime parsing
	layoutCaches []string // per-column datetime layout cache

	// Config flags — must match generic CastVal semantics
	trimSpace      bool
	emptyAsNull    bool
	nullIf         string
	lastBlankCount int          // for skip_blank_lines support
	parseErrLogged map[int]bool // log first parse error per column
}

// NewTargetCastPlan builds a cast plan from known target columns.
// Each column gets a dedicated parser function based on its type.
func NewTargetCastPlan(columns Columns, dateLayouts []string, sp *StreamProcessor) *targetCastPlan {
	plan := &targetCastPlan{
		parsers:        make([]columnParser, len(columns)),
		columns:        columns,
		dateLayouts:    dateLayouts,
		layoutCaches:   make([]string, len(columns)),
		parseErrLogged: make(map[int]bool),
	}

	// Copy config from StreamProcessor to preserve null/trim semantics
	if sp != nil && sp.Config != nil {
		plan.trimSpace = sp.Config.TrimSpace
		plan.emptyAsNull = sp.Config.EmptyAsNull
		plan.nullIf = sp.Config.NullIf
	}

	for i, col := range columns {
		plan.parsers[i] = plan.makeParser(i, col)
	}

	return plan
}

func (p *targetCastPlan) makeParser(colIdx int, col Column) columnParser {
	// Use DbType (native DB type like "int64", "float64", "bool") when available,
	// as this is what processBatch's classifyColumns uses.
	// Fall back to sling's generic ColumnType for broader compatibility.
	dbType := strings.ToLower(col.DbType)

	// Strip nullable/low_cardinality wrappers by peeling matched pairs.
	// Handles nesting like nullable(low_cardinality(string)) or
	// low_cardinality(nullable(decimal(18,2))) without breaking inner parens.
	for {
		if strings.HasPrefix(dbType, "nullable(") {
			dbType = dbType[len("nullable(") : len(dbType)-1]
		} else if strings.HasPrefix(dbType, "low_cardinality(") {
			dbType = dbType[len("low_cardinality(") : len(dbType)-1]
		} else {
			break
		}
	}

	// Match on native DB type first (Proton types)
	switch dbType {
	case "bool":
		return parseBool
	case "int8":
		return parseInt8
	case "int16":
		return parseInt16
	case "int32":
		return parseInt32
	case "int64":
		return parseInt64
	case "uint8":
		return parseUint8
	case "uint16":
		return parseUint16
	case "uint32":
		return parseUint32
	case "uint64":
		return parseUint64
	case "float32":
		return parseFloat32
	case "float64":
		return parseFloat64
	case "string":
		return parseIdentity
	}

	// Match on datetime patterns
	if strings.HasPrefix(dbType, "datetime") || dbType == "date" {
		return p.makeDatetimeParser(colIdx)
	}

	// Match on decimal patterns
	if strings.HasPrefix(dbType, "decimal") {
		return parseDecimal
	}

	// Fallback: use sling's generic ColumnType
	switch {
	case col.Type == BoolType:
		return parseBool
	case col.Type == SmallIntType:
		return parseInt32
	case col.Type == IntegerType || col.Type == BigIntType:
		return parseInt64
	case col.Type == FloatType || col.Type == DecimalType:
		return parseFloat64
	case col.Type.IsDatetime() || col.Type.IsDate():
		return p.makeDatetimeParser(colIdx)
	default:
		return parseIdentity
	}
}

// CastRow converts a row of raw values using precompiled parsers.
// This is the fast path: no stats, no inference, no transform lookup.
// Null/trim semantics match the generic CastVal path.
func (p *targetCastPlan) CastRow(row []any) []any {
	blankCount := 0
	for i := 0; i < len(row) && i < len(p.parsers); i++ {
		val := row[i]
		if val == nil {
			blankCount++
			continue
		}

		// Fast path: value is already a string from CSV
		s, ok := val.(string)
		if !ok {
			s = cast.ToString(val)
		}

		isStringCol := p.columns[i].IsString()

		// Trim: only trim non-string columns, or if TrimSpace is configured.
		// This matches CastVal line 571: if sp.Config.TrimSpace || !col.IsString()
		if p.trimSpace || !isStringCol {
			s = strings.TrimSpace(s)
		}

		// Empty handling — match CastVal lines 577-588
		if s == "" {
			blankCount++
			if p.emptyAsNull || !isStringCol {
				row[i] = nil
			} else {
				row[i] = s // preserve empty string for string columns
			}
			continue
		}

		// NullIf sentinel — match CastVal line 584
		// Note: generic CastVal does NOT count null_if matches as blank,
		// only actual empty strings increment rowBlankValCnt.
		if p.nullIf != "" && s == p.nullIf {
			row[i] = nil
			continue
		}

		parsed, err := p.parsers[i](s)
		if err != nil {
			// Keep original value; processBatch conversion will handle it.
			// Log once per column to surface type mismatches during development.
			if !p.parseErrLogged[i] {
				p.parseErrLogged[i] = true
				g.Debug("fast cast: parse error col %d (%s): %v (value=%q)", i, p.columns[i].Name, err, s)
			}
			continue
		}
		row[i] = parsed
	}

	// Pad row if shorter than target schema
	for len(row) < len(p.parsers) {
		row = append(row, nil)
	}

	// Store blank count for skip_blank_lines support
	p.lastBlankCount = blankCount

	return row
}

// --- Parser functions ---
// These are simple, allocation-minimal parse functions.

func parseIdentity(val string) (any, error) {
	return val, nil
}

func parseBool(val string) (any, error) {
	switch val {
	case "true", "1", "TRUE", "True", "t", "T", "yes", "YES":
		return true, nil
	case "false", "0", "FALSE", "False", "f", "F", "no", "NO":
		return false, nil
	}
	return strconv.ParseBool(val)
}

func parseInt8(val string) (any, error) {
	n, err := strconv.ParseInt(val, 10, 8)
	if err == nil {
		return int8(n), nil
	}
	// Float fallback for "123.0" style values; range-check to avoid silent truncation
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if f < -128 || f > 127 {
		return nil, fmt.Errorf("value %s overflows int8", val)
	}
	return int8(f), nil
}

func parseInt16(val string) (any, error) {
	n, err := strconv.ParseInt(val, 10, 16)
	if err == nil {
		return int16(n), nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if f < -32768 || f > 32767 {
		return nil, fmt.Errorf("value %s overflows int16", val)
	}
	return int16(f), nil
}

func parseInt32(val string) (any, error) {
	n, err := strconv.ParseInt(val, 10, 32)
	if err == nil {
		return int32(n), nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if f < -2147483648 || f > 2147483647 {
		return nil, fmt.Errorf("value %s overflows int32", val)
	}
	return int32(f), nil
}

func parseInt64(val string) (any, error) {
	n, err := strconv.ParseInt(val, 10, 64)
	if err == nil {
		return n, nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	// Note: float64 only has 53 bits of mantissa, so values beyond ±2^53
	// may lose precision. This matches the generic CastVal behavior.
	return int64(f), nil
}

func parseUint8(val string) (any, error) {
	n, err := strconv.ParseUint(val, 10, 8)
	if err == nil {
		return uint8(n), nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if f < 0 || f > 255 {
		return nil, fmt.Errorf("value %s overflows uint8", val)
	}
	return uint8(f), nil
}

func parseUint16(val string) (any, error) {
	n, err := strconv.ParseUint(val, 10, 16)
	if err == nil {
		return uint16(n), nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if f < 0 || f > 65535 {
		return nil, fmt.Errorf("value %s overflows uint16", val)
	}
	return uint16(f), nil
}

func parseUint32(val string) (any, error) {
	n, err := strconv.ParseUint(val, 10, 32)
	if err == nil {
		return uint32(n), nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if f < 0 || f > 4294967295 {
		return nil, fmt.Errorf("value %s overflows uint32", val)
	}
	return uint32(f), nil
}

func parseUint64(val string) (any, error) {
	n, err := strconv.ParseUint(val, 10, 64)
	if err == nil {
		return n, nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	if f < 0 || f > math.MaxUint64 {
		return nil, fmt.Errorf("value %s overflows uint64", val)
	}
	return uint64(f), nil
}

func parseFloat32(val string) (any, error) {
	f, err := strconv.ParseFloat(val, 32)
	if err != nil {
		return nil, err
	}
	return float32(f), nil
}

func parseFloat64(val string) (any, error) {
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func parseDecimal(val string) (any, error) {
	return decimal.NewFromString(val)
}

// makeDatetimeParser creates a closure with per-column layout cache.
// Matches generic ParseTime semantics: date-only values (midnight) are
// converted to UTC to normalize timezone-aware date representations.
func (p *targetCastPlan) makeDatetimeParser(colIdx int) columnParser {
	return func(val string) (any, error) {
		// Try cached layout first (per-column cache)
		if cached := p.layoutCaches[colIdx]; cached != "" {
			t, err := time.Parse(cached, val)
			if err == nil {
				if isDate(&t) {
					t = t.UTC()
				}
				return t, nil
			}
		}

		// Try all known layouts
		for _, layout := range p.dateLayouts {
			t, err := time.Parse(layout, val)
			if err == nil {
				p.layoutCaches[colIdx] = layout
				if isDate(&t) {
					t = t.UTC()
				}
				return t, nil
			}
		}

		// Last resort
		t, err := cast.ToTimeE(val)
		if err != nil {
			return nil, g.Error(err, "cannot parse datetime: %s", val)
		}
		if isDate(&t) {
			t = t.UTC()
		}
		return t, nil
	}
}
