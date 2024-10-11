package iop

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/flarco/g"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
	"golang.org/x/text/encoding/charmap"
	encUnicode "golang.org/x/text/encoding/unicode"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// StreamProcessor processes rows and values
type StreamProcessor struct {
	N                uint64
	dateLayoutCache  string
	stringTypeCache  map[int]string
	colStats         map[int]*ColumnStats
	rowChecksum      []uint64
	unrecognizedDate string
	warn             bool
	parseFuncs       map[string]func(s string) (interface{}, error)
	decReplRegex     *regexp.Regexp
	ds               *Datastream
	dateLayouts      []string
	Config           *StreamConfig
	rowBlankValCnt   int
	transformers     Transformers
	digitString      map[int]string
}

type StreamConfig struct {
	TrimSpace         bool                   `json:"trim_space"`
	EmptyAsNull       bool                   `json:"empty_as_null"`
	Header            bool                   `json:"header"`
	Compression       string                 `json:"compression"` // AUTO | ZIP | GZIP | SNAPPY | NONE
	NullIf            string                 `json:"null_if"`
	NullAs            string                 `json:"null_as"`
	DatetimeFormat    string                 `json:"datetime_format"`
	SkipBlankLines    bool                   `json:"skip_blank_lines"`
	Delimiter         string                 `json:"delimiter"`
	Escape            string                 `json:"escape"`
	Quote             string                 `json:"quote"`
	FileMaxRows       int64                  `json:"file_max_rows"`
	BatchLimit        int64                  `json:"batch_limit"`
	MaxDecimals       int                    `json:"max_decimals"`
	Flatten           bool                   `json:"flatten"`
	FieldsPerRec      int                    `json:"fields_per_rec"`
	Jmespath          string                 `json:"jmespath"`
	BoolAsInt         bool                   `json:"-"`
	Columns           Columns                `json:"columns"` // list of column types. Can be partial list! likely is!
	transforms        map[string][]Transform // array of transform functions to apply
	maxDecimalsFormat string                 `json:"-"`

	Map map[string]string `json:"-"`
}

type Transformers struct {
	Accent transform.Transformer

	DecodeUTF8        transform.Transformer
	DecodeUTF8BOM     transform.Transformer
	DecodeUTF16       transform.Transformer
	DecodeISO8859_1   transform.Transformer
	DecodeISO8859_5   transform.Transformer
	DecodeISO8859_15  transform.Transformer
	DecodeWindows1250 transform.Transformer
	DecodeWindows1252 transform.Transformer

	EncodeUTF8        transform.Transformer
	EncodeUTF8BOM     transform.Transformer
	EncodeUTF16       transform.Transformer
	EncodeISO8859_1   transform.Transformer
	EncodeISO8859_5   transform.Transformer
	EncodeISO8859_15  transform.Transformer
	EncodeWindows1250 transform.Transformer
	EncodeWindows1252 transform.Transformer
}

func NewTransformers() Transformers {
	win16be := encUnicode.UTF16(encUnicode.BigEndian, encUnicode.IgnoreBOM)
	return Transformers{
		Accent: transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC),

		DecodeUTF8:        encUnicode.UTF8.NewDecoder(),
		DecodeUTF8BOM:     encUnicode.UTF8BOM.NewDecoder(),
		DecodeUTF16:       encUnicode.BOMOverride(win16be.NewDecoder()),
		DecodeISO8859_1:   charmap.ISO8859_1.NewDecoder(),
		DecodeISO8859_5:   charmap.ISO8859_5.NewDecoder(),
		DecodeISO8859_15:  charmap.ISO8859_15.NewDecoder(),
		DecodeWindows1250: charmap.Windows1250.NewDecoder(),
		DecodeWindows1252: charmap.Windows1252.NewDecoder(),

		EncodeUTF8:        encUnicode.UTF8.NewEncoder(),
		EncodeUTF8BOM:     encUnicode.UTF8BOM.NewEncoder(),
		EncodeUTF16:       encUnicode.BOMOverride(win16be.NewEncoder()),
		EncodeISO8859_1:   charmap.ISO8859_1.NewEncoder(),
		EncodeISO8859_5:   charmap.ISO8859_5.NewEncoder(),
		EncodeISO8859_15:  charmap.ISO8859_15.NewEncoder(),
		EncodeWindows1250: charmap.Windows1250.NewEncoder(),
		EncodeWindows1252: charmap.Windows1252.NewEncoder(),
	}
}

// NewStreamProcessor returns a new StreamProcessor
func NewStreamProcessor() *StreamProcessor {
	sp := StreamProcessor{
		stringTypeCache: map[int]string{},
		colStats:        map[int]*ColumnStats{},
		decReplRegex:    regexp.MustCompile(`^(\d*[\d.]*?)\.?0*$`),
		transformers:    NewTransformers(),
		digitString:     map[int]string{0: "0"},
	}

	sp.ResetConfig()
	if val := os.Getenv("MAX_DECIMALS"); val != "" && val != "-1" {
		sp.Config.MaxDecimals = cast.ToInt(os.Getenv("MAX_DECIMALS"))
		if sp.Config.MaxDecimals > -1 {
			sp.Config.maxDecimalsFormat = "%." + cast.ToString(sp.Config.MaxDecimals) + "f"
		}
	}

	// if val is '0400', '0401'. Such as codes.
	hasZeroPrefix := func(s string) bool { return len(s) >= 2 && s[0] == '0' && s[1] != '.' }

	sp.parseFuncs = map[string]func(s string) (interface{}, error){
		"int": func(s string) (interface{}, error) {
			if hasZeroPrefix(s) {
				return s, errors.New("number has zero prefix, treat as string")
			}
			// return fastfloat.ParseInt64(s)
			return strconv.ParseInt(s, 10, 64)
		},
		"float": func(s string) (interface{}, error) {
			if hasZeroPrefix(s) {
				return s, errors.New("number has zero prefix, treat as string")
			}
			return strconv.ParseFloat(strings.Replace(s, ",", ".", 1), 64)
		},
		"time": func(s string) (interface{}, error) {
			return sp.ParseTime(s)
		},
		"bool": func(s string) (interface{}, error) {
			return cast.ToBoolE(s)
		},
	}
	sp.dateLayouts = []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05.000000",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02 15:04:05.000 Z",    // snowflake export format
		"2006-01-02 15:04:05.000000 Z", // snowflake export format
		"02-Jan-06",
		"02-Jan-06 15:04:05",
		"02-Jan-06 03:04:05 PM",
		"02-Jan-06 03.04.05.000000 PM",
		"2006-01-02T15:04:05-0700",
		"2006-01-02 15:04:05-07",        // duckdb
		"2006-01-02 15:04:05.000-07",    // duckdb
		"2006-01-02 15:04:05.000000-07", // duckdb
		time.RFC3339,
		"2006-01-02T15:04:05",  // iso8601 without timezone
		"2006-01-02T15:04:05Z", // iso8601 with timezone
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.RFC850,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		"2006-01-02 15:04:05.999999999 -0700 MST", // Time.String()
		"02 Jan 2006",
		"2006-01-02T15:04:05-0700", // RFC3339 without timezone hh:mm colon
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05Z07:00", // RFC3339 without T
		"2006-01-02 15:04:05Z0700",  // RFC3339 without T or timezone hh:mm colon
		"2006-01-02 15:04:05 MST",
		time.Kitchen,
		time.Stamp,
		time.StampMilli,
		time.StampMicro,
		time.StampNano,
		"1/2/06",
		"01/02/06",
		"1/2/2006",
		"01/02/2006",
		"01/02/2006 15:04",
		"01/02/2006 15:04:05",
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006/01/02 15:04:05",
		"02-01-2006",
		"02-01-2006 15:04:05",
	}

	// up to 90 digits. This is done for CastToStringSafeMask
	// shopspring/decimal is buggy and can segfault. Using val.NumDigit,
	// we can create a approximate value mask to output the correct number of bytes
	digitString := "0"
	for i := 1; i <= 90; i++ {
		sp.digitString[i] = digitString
		digitString = digitString + "0"
	}
	return &sp
}

func DefaultStreamConfig() *StreamConfig {
	return &StreamConfig{
		MaxDecimals: -1,
		transforms:  nil,
		Map:         map[string]string{"delimiter": "-1"},
	}
}

func (sp *StreamProcessor) ColStats() map[int]*ColumnStats {
	return sp.colStats
}

func (sp *StreamProcessor) ResetConfig() {
	sp.Config = DefaultStreamConfig()
}

// SetConfig sets the data.Sp.config values
func (sp *StreamProcessor) SetConfig(configMap map[string]string) {
	if sp == nil {
		sp = NewStreamProcessor()
	}

	sp.Config.Map = configMap

	if configMap["fields_per_rec"] != "" {
		sp.Config.FieldsPerRec = cast.ToInt(configMap["fields_per_rec"])
	}

	if configMap["delimiter"] != "" {
		sp.Config.Delimiter = configMap["delimiter"]
	}

	if configMap["escape"] != "" {
		sp.Config.Escape = configMap["escape"]
	}

	if configMap["quote"] != "" {
		sp.Config.Quote = configMap["quote"]
	}

	if configMap["file_max_rows"] != "" {
		sp.Config.FileMaxRows = cast.ToInt64(configMap["file_max_rows"])
	}

	if configMap["batch_limit"] != "" {
		sp.Config.BatchLimit = cast.ToInt64(configMap["batch_limit"])
	}

	if configMap["header"] != "" {
		sp.Config.Header = cast.ToBool(configMap["header"])
	} else {
		sp.Config.Header = true
	}

	if configMap["flatten"] != "" {
		sp.Config.Flatten = cast.ToBool(configMap["flatten"])
	}

	if configMap["max_decimals"] != "" && configMap["max_decimals"] != "-1" {
		var err error
		sp.Config.MaxDecimals, err = cast.ToIntE(configMap["max_decimals"])
		if err != nil {
			sp.Config.MaxDecimals = -1
		} else if sp.Config.MaxDecimals > -1 {
			sp.Config.maxDecimalsFormat = "%." + cast.ToString(sp.Config.MaxDecimals) + "f"
		}
	}

	if configMap["empty_as_null"] != "" {
		sp.Config.EmptyAsNull = cast.ToBool(configMap["empty_as_null"])
	}
	if configMap["null_if"] != "" {
		sp.Config.NullIf = configMap["null_if"]
	}
	if configMap["null_as"] != "" {
		sp.Config.NullAs = configMap["null_as"]
	}
	if configMap["trim_space"] != "" {
		sp.Config.TrimSpace = cast.ToBool(configMap["trim_space"])
	}
	if configMap["jmespath"] != "" {
		sp.Config.Jmespath = cast.ToString(configMap["jmespath"])
	}
	if configMap["skip_blank_lines"] != "" {
		sp.Config.SkipBlankLines = cast.ToBool(configMap["skip_blank_lines"])
	}
	if configMap["bool_at_int"] != "" {
		sp.Config.BoolAsInt = cast.ToBool(configMap["bool_at_int"])
	}
	if configMap["columns"] != "" {
		g.Unmarshal(configMap["columns"], &sp.Config.Columns)
	}
	if configMap["transforms"] != "" {
		sp.applyTransforms(configMap["transforms"])
	}
	sp.Config.Compression = configMap["compression"]

	if configMap["datetime_format"] != "" {
		sp.Config.DatetimeFormat = Iso8601ToGoLayout(configMap["datetime_format"])
		// put in first
		sp.dateLayouts = append(
			[]string{sp.Config.DatetimeFormat},
			sp.dateLayouts...)
	}
}

func makeColumnTransforms(transformsPayload string) map[string][]string {
	columnTransforms := map[string][]string{}
	g.Unmarshal(transformsPayload, &columnTransforms)
	return columnTransforms
}

func (sp *StreamProcessor) applyTransforms(transformsPayload string) {
	columnTransforms := makeColumnTransforms(transformsPayload)
	sp.Config.transforms = map[string][]Transform{}
	for key, names := range columnTransforms {
		sp.Config.transforms[key] = []Transform{}
		for _, name := range names {
			t, ok := TransformsMap[name]
			if ok {
				sp.Config.transforms[key] = append(sp.Config.transforms[key], t)
			} else if n := strings.TrimSpace(string(name)); strings.Contains(n, "(") && strings.HasSuffix(n, ")") {
				// parse transform with a parameter
				parts := strings.Split(string(name), "(")
				if len(parts) != 2 {
					g.Warn("invalid transform: '%s'", name)
					continue
				}
				tName := parts[0]
				param := strings.TrimSuffix(parts[1], ")")
				if t, ok := TransformsMap[tName]; ok {
					if t.makeFunc == nil {
						g.Warn("makeFunc not found for transform '%s'. Please contact support", tName)
						continue
					}
					err := t.makeFunc(&t, param)
					if err != nil {
						g.Warn("invalid parameter for transform '%s' (%s)", tName, err.Error())
					} else {
						sp.Config.transforms[key] = append(sp.Config.transforms[key], t)
					}
				} else {
					g.Warn("did find find transform with params named: '%s'", tName)
				}
			} else {
				g.Warn("did find find transform named: '%s'", name)
			}
		}
	}
}

// CastVal  casts the type of an interface based on its value
// From html/template/content.go
// Copyright 2011 The Go Authors. All rights reserved.
// indirect returns the value, after dereferencing as many times
// as necessary to reach the base type (or nil).
func (sp *StreamProcessor) indirect(a interface{}) interface{} {
	if a == nil {
		return nil
	}
	if t := reflect.TypeOf(a); t.Kind() != reflect.Ptr {
		// Avoid creating a reflect.Value if it's not a pointer.
		return a
	}
	v := reflect.ValueOf(a)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

func (sp *StreamProcessor) toFloat64E(i interface{}) (float64, error) {
	i = sp.indirect(i)

	switch s := i.(type) {
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case string:
		v, err := strconv.ParseFloat(strings.Replace(s, ",", ".", 1), 64)
		if err == nil {
			return v, nil
		}
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	case []uint8:
		v, err := strconv.ParseFloat(strings.Replace(string(s), ",", ".", 1), 64)
		if err == nil {
			return v, nil
		}
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	case int:
		return float64(s), nil
	case int64:
		return float64(s), nil
	case int32:
		return float64(s), nil
	case int16:
		return float64(s), nil
	case int8:
		return float64(s), nil
	case uint:
		return float64(s), nil
	case uint64:
		return float64(s), nil
	case uint32:
		return float64(s), nil
	case uint16:
		return float64(s), nil
	case uint8:
		return float64(s), nil
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	}
}

// CastType casts the type of an interface
// CastType is used to cast the interface place holders?
func (sp *StreamProcessor) CastType(val interface{}, typ ColumnType) interface{} {
	var nVal interface{}

	switch {
	case typ.IsBinary():
		nVal = []byte(cast.ToString(val))
	case typ.IsString():
		nVal = cast.ToString(val)
	case typ == SmallIntType:
		nVal = cast.ToInt(val)
	case typ.IsInteger():
		nVal = cast.ToInt64(val)
	case typ.IsFloat():
		nVal = val
	case typ.IsDecimal():
		nVal = val
	case typ.IsBool():
		// nVal = cast.ToBool(val)
		nVal = val
	case typ.IsDatetime():
		nVal = cast.ToTime(val)
	default:
		nVal = cast.ToString(val)
	}

	return nVal
}

// GetType returns the type of an interface
func (sp *StreamProcessor) GetType(val interface{}) (typ ColumnType) {
	data := NewDataset(NewColumnsFromFields("col"))
	data.Append([]any{val})
	if ds := sp.ds; ds != nil {
		data.SafeInference = ds.SafeInference
		data.Sp.dateLayouts = ds.Sp.dateLayouts
	}
	data.InferColumnTypes()
	return data.Columns[0].Type
}

// commitChecksum increments the checksum. This is needed due to reprocessing rows
func (sp *StreamProcessor) commitChecksum() {
	for i, val := range sp.rowChecksum {
		cs, ok := sp.colStats[i]
		if !ok {
			sp.colStats[i] = &ColumnStats{}
			cs = sp.colStats[i]
		}
		cs.Checksum = cs.Checksum + val
		sp.ds.Columns[i].Stats.Checksum = cs.Checksum
	}
}

// CastVal casts values with stats collection
// which degrades performance by ~10%
// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessVal'
func (sp *StreamProcessor) CastVal(i int, val interface{}, col *Column) interface{} {
	cs, ok := sp.colStats[i]
	if !ok {
		sp.colStats[i] = &ColumnStats{}
		cs = sp.colStats[i]
	}

	var nVal interface{}
	var sVal string
	isString := false

	if val == nil {
		cs.TotalCnt++
		cs.NullCnt++
		sp.rowBlankValCnt++
		return nil
	}

	switch v := val.(type) {
	case big.Int:
		val = v.Int64()
	case *big.Int:
		val = v.Int64()
	case []uint8:
		sVal = string(v)
		val = sVal
		isString = true
	case string, *string:
		switch v2 := v.(type) {
		case string:
			sVal = v2
		case *string:
			sVal = *v2
		}

		isString = true
		if sp.Config.TrimSpace || !col.IsString() {
			// if colType is not string, and the value is string, we should trim it
			// in case it comes from a CSV. If it's empty, it should be considered nil
			sVal = strings.TrimSpace(sVal)
			val = sVal
		}
		if sVal == "" {
			sp.rowBlankValCnt++
			if sp.Config.EmptyAsNull || !col.IsString() {
				cs.TotalCnt++
				cs.NullCnt++
				return nil
			}
		} else if sp.Config.NullIf == sVal {
			cs.TotalCnt++
			cs.NullCnt++
			return nil
		}
	}

	// get transforms
	key := strings.ToLower(col.Name)
	transforms := append(sp.Config.transforms[key], sp.Config.transforms["*"]...)

	switch {
	case col.Type.IsString():
		if sVal == "" && val != nil {
			if reflect.TypeOf(val).Kind() == reflect.Slice || reflect.TypeOf(val).Kind() == reflect.Map {
				sVal = g.Marshal(val)
			} else {
				sVal = cast.ToString(val)
			}
		}

		// apply transforms
		for _, t := range transforms {
			if t.FuncString != nil {
				sVal, _ = t.FuncString(sp, sVal)
			}
		}

		l := len(sVal)
		if l > cs.MaxLen {
			cs.MaxLen = l
		}

		if looksLikeJson(sVal) {
			cs.JsonCnt++
			sp.rowChecksum[i] = uint64(len(strings.ReplaceAll(sVal, " ", "")))
			cs.TotalCnt++
			return sVal
		}

		// above 4000 is considered text
		if l > 4000 && !g.In(col.Type, TextType, BinaryType) {
			sp.ds.ChangeColumn(i, TextType) // change to text
		}

		cond1 := cs.TotalCnt > 0 && cs.NullCnt == cs.TotalCnt
		cond2 := !isString && cs.StringCnt == 0

		if (cond1 || cond2) && sp.ds != nil && !col.Sourced {
			// this is an attempt to cast correctly "uncasted" columns
			// (defaulting at string). This will not work in most db insert cases,
			// as the ds.Shape() function will change it back to the "string" type,
			// to match the target table column type. This takes priority.
			nVal = sp.ParseString(cast.ToString(val))
			sp.ds.ChangeColumn(i, sp.GetType(nVal))
			if !sp.ds.Columns[i].IsString() { // so we don't loop
				return sp.CastVal(i, nVal, &sp.ds.Columns[i])
			}
			cs.StringCnt++
			sp.rowChecksum[i] = uint64(len(sVal))
			nVal = sVal
		} else {
			if col.Type == JsonType && !col.Sourced {
				sp.ds.ChangeColumn(i, StringType) // change to string, since it's not really json
			}
			cs.StringCnt++
			sp.rowChecksum[i] = uint64(len(sVal))
			if col.Type == BinaryType {
				nVal = []byte(sVal)
			} else if col.Type == JsonType && sVal == "" {
				nVal = nil // if json, empty should be null
			} else {
				nVal = sVal
			}
		}
	case col.Type == SmallIntType:
		iVal, err := cast.ToInt32E(val)
		if err != nil {
			fVal, err := sp.toFloat64E(val)
			if err != nil || sp.ds == nil {
				// is string
				sp.ds.ChangeColumn(i, StringType)
				cs.StringCnt++
				cs.TotalCnt++
				sVal = cast.ToString(val)
				sp.rowChecksum[i] = uint64(len(sVal))
				return sVal
			}
			// is decimal
			sp.ds.ChangeColumn(i, DecimalType)
			return fVal
		}

		if int64(iVal) > cs.Max {
			cs.Max = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			sp.rowChecksum[i] = uint64(-iVal)
		} else {
			sp.rowChecksum[i] = uint64(iVal)
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		nVal = iVal
	case col.Type.IsInteger():
		iVal, err := cast.ToInt64E(val)
		if err != nil {
			fVal, err := sp.toFloat64E(val)
			if err != nil || sp.ds == nil {
				// is string
				sp.ds.ChangeColumn(i, StringType)
				cs.StringCnt++
				cs.TotalCnt++
				sVal = cast.ToString(val)
				sp.rowChecksum[i] = uint64(len(sVal))
				return sVal
			}
			// is decimal
			sp.ds.ChangeColumn(i, DecimalType)
			return fVal
		}

		if iVal > cs.Max {
			cs.Max = iVal
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			sp.rowChecksum[i] = uint64(-iVal)
		} else {
			sp.rowChecksum[i] = uint64(iVal)
		}
		nVal = iVal
	case col.Type == FloatType:
		fVal, err := sp.toFloat64E(val)
		if err == nil && math.IsNaN(fVal) {
			// set as null
			cs.NullCnt++
			return nil
		} else if err != nil {
			// is string
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			cs.TotalCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			return sVal
		}

		cs.DecCnt++
		if fVal < 0 {
			sp.rowChecksum[i] = uint64(-fVal)
		} else {
			sp.rowChecksum[i] = uint64(fVal)
		}
		nVal = fVal

	case col.Type.IsNumber():
		fVal, err := sp.toFloat64E(val)
		if err == nil && math.IsNaN(fVal) {
			// set as null
			cs.NullCnt++
			return nil
		} else if err != nil {
			// is string
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			cs.TotalCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			return sVal
		}

		intVal := int64(fVal)
		if intVal > cs.Max {
			cs.Max = intVal
		}
		if intVal < cs.Min {
			cs.Min = intVal
		}

		if fVal < 0 {
			sp.rowChecksum[i] = uint64(-fVal)
		} else {
			sp.rowChecksum[i] = uint64(fVal)
		}

		isInt := float64(intVal) == fVal
		if isInt {
			cs.IntCnt++ // is an integer
		} else {
			cs.DecCnt++
		}

		if sp.Config.MaxDecimals > -1 && !isInt {
			nVal = g.F(sp.Config.maxDecimalsFormat, fVal)
		} else {
			nVal = strings.Replace(cast.ToString(val), ",", ".", 1) // use string to keep accuracy, replace comma as decimal point
		}

	case col.Type.IsBool():
		var err error
		bVal, err := sp.CastToBool(val)
		if err != nil {
			// is string
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			cs.TotalCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			return sVal
		} else {
			nVal = strconv.FormatBool(bVal) // keep as string
			sp.rowChecksum[i] = uint64(len(nVal.(string)))
		}

		cs.BoolCnt++
	case col.Type.IsDatetime() || col.Type.IsDate():
		dVal, err := sp.CastToTime(val)
		if err != nil {
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			nVal = sVal
		} else if dVal.IsZero() {
			nVal = nil
			cs.NullCnt++
			sp.rowBlankValCnt++
		} else {
			// apply transforms
			for _, t := range transforms {
				if t.FuncTime != nil {
					_ = t.FuncTime(sp, &dVal)
				}

				// column needs to be set to timestampz
				if t.Name == "set_timezone" && col.Type != TimestampzType {
					sp.ds.ChangeColumn(i, TimestampzType)
				}
			}
			nVal = dVal
			if isDate(&dVal) {
				cs.DateCnt++
			} else if isUTC(&dVal) {
				cs.DateTimeCnt++
			} else {
				cs.DateTimeZCnt++
			}
			sp.rowChecksum[i] = uint64(dVal.UnixMicro())
		}
	}
	cs.TotalCnt++
	return nVal
}

// CastToString to string. used for csv writing
// slows processing down 5% with upstream CastRow or 35% without upstream CastRow
func (sp *StreamProcessor) CastToString(i int, val interface{}, valType ...ColumnType) string {
	typ := ColumnType("")
	switch v := val.(type) {
	case time.Time:
		typ = DatetimeType
	default:
		_ = v
	}

	if len(valType) > 0 {
		typ = valType[0]
	}

	switch {
	case val == nil:
		return sp.Config.NullAs
	case sp.Config.BoolAsInt && typ.IsBool():
		switch cast.ToString(val) {
		case "true", "1", "TRUE":
			return "1"
		}
		return "0"
	case typ.IsDecimal() || typ.IsFloat():
		if RemoveTrailingDecZeros {
			// attempt to remove trailing zeros, but is 10 times slower
			return sp.decReplRegex.ReplaceAllString(cast.ToString(val), "$1")
		}
		return cast.ToString(val)
		// return fmt.Sprintf("%v", val)
	case typ.IsDate():
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		}
		return tVal.Format("2006-01-02")
	case typ.IsDatetime():
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		} else if sp.Config.DatetimeFormat != "" && strings.ToLower(sp.Config.DatetimeFormat) != "auto" {
			return tVal.Format(sp.Config.DatetimeFormat)
		} else if tVal.Location() == nil {
			return tVal.Format("2006-01-02 15:04:05.000000") + " +00"
		}
		return tVal.Format("2006-01-02 15:04:05.000000 -07")
	default:
		return cast.ToString(val)
	}
}

// CastToStringSafe to string (safer)
func (sp *StreamProcessor) CastToStringSafe(i int, val interface{}, valType ...ColumnType) string {
	typ := ColumnType("")
	switch v := val.(type) {
	case time.Time:
		typ = DatetimeType
	default:
		_ = v
	}

	if len(valType) > 0 {
		typ = valType[0]
	}

	switch {
	case val == nil:
		return ""
	case sp.Config.BoolAsInt && typ.IsBool():
		switch cast.ToString(val) {
		case "true", "1", "TRUE":
			return "1"
		}
		return "0"
	case typ.IsDecimal() || typ.IsFloat():
		return cast.ToString(val)
	case typ.IsDate():
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		}
		return tVal.UTC().Format("2006-01-02")
	case typ.IsDatetime():
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		}
		return tVal.UTC().Format("2006-01-02 15:04:05.000000") + " +00"
	default:
		return cast.ToString(val)
	}
}

// CastToStringSafe to masks to count bytes (even safer)
func (sp *StreamProcessor) CastToStringSafeMask(i int, val interface{}, valType ...ColumnType) string {
	typ := ColumnType("")
	switch v := val.(type) {
	case time.Time:
		typ = DatetimeType
	default:
		_ = v
	}

	if len(valType) > 0 {
		typ = valType[0]
	}

	switch {
	case val == nil:
		return ""
	case sp.Config.BoolAsInt && typ.IsBool():
		return "0" // as a mask
	case typ.IsBool():
		return cast.ToString(val)
	case typ.IsDecimal() || typ.IsFloat():
		if valD, ok := val.(decimal.Decimal); ok {
			// shopspring/decimal is buggy and can segfault. Using val.NumDigit,
			// we can create a approximate value mask to output the correct number of bytes
			return sp.digitString[valD.NumDigits()]
		}
		return cast.ToString(val)
	case typ.IsDate():
		return "2006-01-02" // as a mask
	case typ.IsDatetime():
		return "2006-01-02 15:04:05.000000 +00" // as a mask
	default:
		return cast.ToString(val)
	}
}

// CastValWithoutStats casts the value without counting stats
func (sp *StreamProcessor) CastValWithoutStats(i int, val interface{}, typ ColumnType) interface{} {
	var nVal interface{}
	if nil == val {
		return nil
	}

	switch v := val.(type) {
	case []uint8:
		val = cast.ToString(val)
	default:
		_ = v
	}

	switch typ {
	case "string", "text", "json", "time", "bytes":
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	case "smallint":
		nVal = cast.ToInt(val)
	case "integer", "bigint":
		nVal = cast.ToInt64(val)
	case "decimal", "float":
		// max 9 decimals for bigquery compatibility
		// nVal = math.Round(fVal*1000000000) / 1000000000
		nVal = val // use string to keep accuracy
	case "bool":
		nVal = cast.ToBool(val)
	case "datetime", "date", "timestamp", "timestampz":
		dVal, err := sp.CastToTime(val)
		if err != nil {
			nVal = val // keep string
		} else if dVal.IsZero() {
			nVal = nil
		} else {
			nVal = dVal
		}
	default:
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	}

	return nVal
}

// CastToBool converts interface to bool
func (sp *StreamProcessor) CastToBool(i interface{}) (b bool, err error) {
	i = sp.indirect(i)

	switch b := i.(type) {
	case bool:
		return b, nil
	case nil:
		return false, nil
	case int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8, float64, float32:
		return cast.ToInt(i) != 0, nil
	case string:
		return strconv.ParseBool(i.(string))
	case json.Number:
		v, err := cast.ToInt64E(b)
		if err == nil {
			return v != 0, nil
		}
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", i, i)
	default:
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", i, i)
	}
}

// CastToTime converts interface to time
func (sp *StreamProcessor) CastToTime(i interface{}) (t time.Time, err error) {
	i = sp.indirect(i)
	switch v := i.(type) {
	case nil:
		return
	case time.Time:
		return v, nil
	case string:
		return sp.ParseTime(i.(string))
	default:
		return cast.ToTimeE(i)
	}
}

// ParseTime parses a date string and returns time.Time
func (sp *StreamProcessor) ParseTime(i interface{}) (t time.Time, err error) {
	s := cast.ToString(i)
	if s == "" {
		return t, nil // return zero time, so it become nil
	}

	// date layouts to try out
	for _, layout := range sp.dateLayouts {
		// use cache to decrease parsing computation next iteration
		if sp.dateLayoutCache != "" {
			t, err = time.Parse(sp.dateLayoutCache, s)
			if err == nil {
				if isDate(&t) {
					t = t.UTC() // convert to utc for dates
				}
				return
			}
		}
		t, err = time.Parse(layout, s)
		if err == nil {
			sp.dateLayoutCache = layout
			if isDate(&t) {
				t = t.UTC() // convert to utc for dates
			}
			return
		}
	}
	return
}

// ParseString return an interface
// string: "varchar"
// integer: "integer"
// decimal: "decimal"
// date: "date"
// datetime: "timestamp"
// timestamp: "timestamp"
// text: "text"
func (sp *StreamProcessor) ParseString(s string, jj ...int) interface{} {
	if s == "" {
		return nil
	}

	j := -1
	if len(jj) > 0 {
		j = jj[0]
	}

	stringTypeCache := sp.stringTypeCache[j]

	if stringTypeCache != "" {
		i, err := sp.parseFuncs[stringTypeCache](s)
		if err == nil {
			return i
		}
	}

	// int
	i, err := sp.parseFuncs["int"](s)
	if err == nil {
		// if s = 0100, casting to int64 will return 64
		// need to mitigate by when s starts with 0
		if len(s) > 1 && s[0] == '0' {
			return s
		}
		sp.stringTypeCache[j] = "int"
		return i
	}

	// float
	f, err := sp.parseFuncs["float"](s)
	if err == nil {
		sp.stringTypeCache[j] = "float"
		return f
	}

	// date/time
	t, err := sp.parseFuncs["time"](s)
	if err == nil {
		sp.stringTypeCache[j] = "time"
		return t
	}

	// boolean
	// FIXME: causes issues in SQLite and Oracle, needed for correct boolean parsing
	b, err := sp.parseFuncs["bool"](s)
	if err == nil {
		sp.stringTypeCache[j] = "bool"
		return b
	}

	return s
}

// ProcessVal processes a value
func (sp *StreamProcessor) ProcessVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case []uint8:
		nVal = cast.ToString(val)
	default:
		nVal = val
		_ = v
	}
	return nVal

}

// ParseVal parses the value into its appropriate type
func (sp *StreamProcessor) ParseVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case time.Time:
		nVal = cast.ToTime(val)
	case nil:
		nVal = val
	case int:
		nVal = cast.ToInt64(val)
	case int8:
		nVal = cast.ToInt64(val)
	case int16:
		nVal = cast.ToInt64(val)
	case int32:
		nVal = cast.ToInt64(val)
	case int64:
		nVal = cast.ToInt64(val)
	case float32:
		nVal = cast.ToFloat32(val)
	case float64:
		nVal = cast.ToFloat64(val)
	case bool:
		nVal = cast.ToBool(val)
	case []uint8:
		nVal = sp.ParseString(cast.ToString(val))
	default:
		nVal = sp.ParseString(cast.ToString(val))
		_ = v
		// fmt.Printf("%T\n", val)
	}
	return nVal
}

// CastRow casts each value of a row
// slows down processing about 40%?
func (sp *StreamProcessor) CastRow(row []interface{}, columns Columns) []interface{} {
	sp.N++
	// Ensure usable types
	sp.rowBlankValCnt = 0
	sp.rowChecksum = make([]uint64, len(row))
	for i, val := range row {
		col := &columns[i]
		row[i] = sp.CastVal(i, val, col)

		// evaluate constraint
		if col.Constraint != nil {
			col.EvaluateConstraint(row[i], sp)
		}
	}

	for len(row) < len(columns) {
		row = append(row, nil)
	}

	// debug a row, prev
	if sp.warn {
		g.Trace("%s -> %#v", sp.unrecognizedDate, row)
		sp.warn = false
	}

	return row
}

// ProcessRow processes a row
func (sp *StreamProcessor) ProcessRow(row []interface{}) []interface{} {
	// Ensure usable types
	for i, val := range row {
		row[i] = sp.ProcessVal(val)
	}
	return row
}
