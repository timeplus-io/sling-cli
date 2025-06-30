package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/fatih/color"
	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/timeplus-io/proton-go-driver/v2"
	_ "github.com/timeplus-io/proton-go-driver/v2"

	"github.com/flarco/g"
)

const (
	maxRetries        = 4
	retryDelay        = 5 * time.Second
	countWaitDuration = 5 * time.Second
)

var protonDataTypeMap = map[string]string{
	// Basic integer types
	"int8":   "int8",
	"int16":  "int16",
	"int32":  "int32",
	"int64":  "int64",
	"uint8":  "uint8",
	"uint16": "uint16",
	"uint32": "uint32",
	"uint64": "uint64",

	// Floating point types
	"float32": "float32",
	"float64": "float64",

	// String and boolean types
	"string":  "string",
	"bool":    "bool",
	"boolean": "bool", // alias

	// Decimal types for precise calculations
	"decimal":       "decimal",
	"decimal32":     "decimal32",
	"decimal64":     "decimal64",
	"decimal128":    "decimal128",
	"decimal256":    "decimal256",
	"decimal(9,2)":  "decimal32",  // Common precision
	"decimal(18,4)": "decimal64",  // Higher precision
	"decimal(38,8)": "decimal128", // Very high precision
	"numeric":       "decimal",    // PostgreSQL compatibility
	"money":         "decimal64",  // Money type alias

	// Date/Time types for temporal data
	"date":          "date",
	"date32":        "date32",
	"datetime":      "datetime",
	"datetime64":    "datetime64",
	"datetime64(3)": "datetime64", // Millisecond precision
	"datetime64(6)": "datetime64", // Microsecond precision
	"timestamp":     "datetime64", // Common alias

	// UUID type for unique identifiers
	"uuid": "uuid",

	// Map types
	"map(string, string)":         "map(string, string)",
	"map(string, int32)":          "map(string, int32)",
	"map(string, int64)":          "map(string, int64)",
	"map(string, float64)":        "map(string, float64)",
	"map(string, array(string))":  "map(string, array(string))",
	"map(string, array(float32))": "map(string, array(float32))",
	"map(int32, string)":          "map(int32, string)",
	"map(int64, string)":          "map(int64, string)",

	// Array types
	"array(int8)":       "array(int8)",
	"array(int16)":      "array(int16)",
	"array(int32)":      "array(int32)",
	"array(int64)":      "array(int64)",
	"array(uint8)":      "array(uint8)",
	"array(uint16)":     "array(uint16)",
	"array(uint32)":     "array(uint32)",
	"array(uint64)":     "array(uint64)",
	"array(float32)":    "array(float32)",
	"array(float64)":    "array(float64)",
	"array(string)":     "array(string)",
	"array(bool)":       "array(bool)",
	"array(boolean)":    "array(bool)",
	"array(decimal32)":  "array(decimal32)",  // Decimal arrays
	"array(decimal64)":  "array(decimal64)",  // Decimal arrays
	"array(date)":       "array(date)",       // Date arrays
	"array(datetime64)": "array(datetime64)", // Timestamp arrays
	"array(uuid)":       "array(uuid)",       // UUID arrays
}

// ProtonConn is a Proton connection
type ProtonConn struct {
	BaseConn
	URL              string
	Idempotent       bool
	IdempotentPrefix string
	ProtonConn       proton.Conn
	retryBackoff     *backoff.ExponentialBackOff
}

// Init initiates the object
func (conn *ProtonConn) Init() error {
	u, err := url.Parse(conn.URL)
	if err != nil {
		return g.Error(err, "could not parse Proton URL")
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "8463" // default Proton port
	}

	username := u.User.Username()
	password, _ := u.User.Password() // Password might be empty
	database := strings.TrimPrefix(u.Path, "/")

	conn.ProtonConn, err = proton.Open(&proton.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: proton.Auth{
			Database: database,
			Username: username,
			Password: password, // This might be an empty string
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return g.Error(err, "could not connect to proton")
	}

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbProton

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	conn.retryBackoff = backoff.NewExponentialBackOff()
	conn.retryBackoff.MaxElapsedTime = 5 * time.Minute
	conn.retryBackoff.InitialInterval = 1 * time.Second
	return conn.BaseConn.Init()
}

func (conn *ProtonConn) Connect(timeOut ...int) (err error) {

	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected packet") {
			g.Info(color.MagentaString("Try using the `http_url` instead to connect to Proton via HTTP. See https://docs.slingdata.io/connections/database-connections/Proton"))
		}
	}

	return err
}

func (conn *ProtonConn) ConnString() string {

	if url := conn.GetProp("http_url"); url != "" {
		return url
	}

	return conn.BaseConn.ConnString()
}

// SetIdempotent enables or disables idempotent support
func (conn *ProtonConn) SetIdempotent(enabled bool, prefix string) {
	conn.Idempotent = enabled
	conn.IdempotentPrefix = prefix
}

// NewTransaction creates a new transaction
func (conn *ProtonConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error) {

	context := g.NewContext(ctx)

	if len(options) == 0 {
		options = []*sql.TxOptions{&sql.TxOptions{}}
	}

	tx, err := conn.Db().BeginTxx(context.Ctx, options[0])
	if err != nil {
		return nil, g.Error(err, "could not begin Tx")
	}

	Tx := &BaseTransaction{Tx: tx, Conn: conn.Self(), context: &context}
	conn.tx = Tx

	// ProtonDB does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}

	return Tx, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *ProtonConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	partitionBy := ""
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		// allow custom SQL expression for partitioning
		partitionBy = g.F("partition by (%s)", strings.Join(keys, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		partitionBy = g.F("partition by %s", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{partition_by}", partitionBy)

	return strings.TrimSpace(sql), nil
}

// Define a helper function for retrying operations
func retryWithBackoff(operation func() error) error {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 5 * time.Minute // Set a maximum total retry time
	b.InitialInterval = 1 * time.Second

	return backoff.RetryNotify(operation, b, func(err error, duration time.Duration) {
		g.Warn("Operation failed, retrying in %v: %v", duration, err)
	})
}

// BulkImportStream inserts a stream into a table
func (conn *ProtonConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	// set default schema
	conn.Exec(g.F("use `%s`", table.Schema))

	// set OnSchemaChange
	if df := ds.Df(); df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {
		oldOnColumnChanged := df.OnColumnChanged
		df.OnColumnChanged = func(col iop.Column) error {

			// sleep to allow transaction to close
			time.Sleep(100 * time.Millisecond)

			ds.Context.Lock()
			defer ds.Context.Unlock()

			// use pre-defined function
			err = oldOnColumnChanged(col)
			if err != nil {
				return g.Error(err, "could not process ColumnChange for Postgres")
			}

			return nil
		}
	}

	batchCount := 0
	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}
		}

		batchCount++
		err = conn.processBatch(tableFName, table, batch, columns, batchCount, &count, ds)
		if err != nil {
			if permanentErr, ok := err.(*backoff.PermanentError); ok {
				return count, g.Error(permanentErr.Err, "permanent error processing batch %d", batchCount)
			}
			return count, g.Error(err, "failed to process batch %d after retries", batchCount)
		}
	}

	ds.SetEmpty()

	g.Debug("%d ROWS COPIED", count)
	return count, nil
}

// processBatch handles the processing of a single batch
func (conn *ProtonConn) processBatch(tableFName string, table Table, batch *iop.Batch, columns iop.Columns, batchCount int, count *uint64, ds *iop.Datastream) error {
	batchRows := make([][]any, 0, batch.Count)
	for row := range batch.Rows {
		batchRows = append(batchRows, row)
	}

	idPrefix := fmt.Sprintf("%d_%d_batch_%s",
		time.Now().UnixNano(),
		batchCount,
		table.FullName())
	conn.SetIdempotent(true, idPrefix)

	operation := func() error {
		// Check context cancellation
		select {
		case <-ds.Context.Ctx.Done():
			return backoff.Permanent(g.Error(ds.Context.Ctx.Err(), "context cancelled"))
		default:
		}

		var err error
		if conn.Tx() == nil {
			err = conn.Begin(&sql.TxOptions{Isolation: sql.LevelDefault})
			if err != nil {
				return backoff.Permanent(g.Error(err, "could not begin transaction"))
			}
			defer func() {
				if err != nil {
					if rollbackErr := conn.Rollback(); rollbackErr != nil {
						g.Warn("Failed to rollback transaction: %v", rollbackErr)
					}
				}
			}()
		}

		insFields, err := conn.ValidateColumnNames(columns, batch.Columns.Names(), true)
		if err != nil {
			return backoff.Permanent(g.Error(err, "columns mismatch"))
		}

		insertStatement := conn.GenerateInsertStatement(
			table.FullName(),
			insFields,
			1,
		)

		batched, err := conn.ProtonConn.PrepareBatch(ds.Context.Ctx, insertStatement)
		if err != nil {
			return g.Error(err, "could not prepare statement for table: %s, statement: %s", table.FullName(), insertStatement)
		}

		// Build column converters based on column types
		columnConverters := conn.buildColumnConverters(batch.Columns)

		// Counter for successfully inserts within this batch
		var internalCount uint64
		for _, row := range batchRows {
			var eG g.ErrorGroup

			// Apply all column conversions
			for colIndex, converter := range columnConverters {
				if row[colIndex] != nil {
					convertedValue, convertErr := converter.Convert(row[colIndex])
					if convertErr != nil {
						eG.Capture(g.Error(convertErr, "failed to convert column %d (%s)", colIndex, converter.TypeName))
					} else {
						row[colIndex] = convertedValue
					}
				} else {
					g.Debug("%s column %d value == nil", converter.TypeName, colIndex)
				}
			}

			if err = eG.Err(); err != nil {
				err = g.Error(err, "could not convert value for COPY into table %s", tableFName)
				ds.Context.CaptureErr(err)
				return backoff.Permanent(err) // Type conversion errors are permanent
			}

			// Do insert
			ds.Context.Lock()
			err = batched.Append(row...)
			ds.Context.Unlock()
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "could not insert into table %s, row: %#v", tableFName, row))
				return g.Error(err, "could not execute statement") // Network/temporary errors can retry
			}
			internalCount++
		}

		err = batched.Send()
		if err != nil {
			return g.Error(err, "could not send batch data")
		}

		err = conn.Commit()
		if err != nil {
			return g.Error(err, "could not commit transaction")
		}

		// Update count only after successful commit
		*count += internalCount
		return nil
	}

	return backoff.RetryNotify(operation,
		conn.retryBackoff,
		func(err error, duration time.Duration) {
			g.Warn("Batch %d failed, retrying in %v: %v", batchCount, duration, err)
		})
}

// ColumnConverter defines how to convert a column value
type ColumnConverter struct {
	TypeName string
	Convert  func(value interface{}) (interface{}, error)
}

// buildColumnConverters creates a map of column converters based on column types
func (conn *ProtonConn) buildColumnConverters(columns iop.Columns) map[int]ColumnConverter {
	converters := make(map[int]ColumnConverter)

	for i, col := range columns {
		originalDbType := strings.ToLower(col.DbType)
		dbType := originalDbType

		// First, check for exact matches with nested nullable types (before wrapper stripping)
		switch originalDbType {
		// Array with nullable elements - these require pointer slices
		case "array(nullable(int8))":
			converters[i] = ColumnConverter{"array(nullable(int8))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableInt8(v)
			}}
			continue
		case "array(nullable(int16))":
			converters[i] = ColumnConverter{"array(nullable(int16))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableInt16(v)
			}}
			continue
		case "array(nullable(int32))":
			converters[i] = ColumnConverter{"array(nullable(int32))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableInt32(v)
			}}
			continue
		case "array(nullable(int64))":
			converters[i] = ColumnConverter{"array(nullable(int64))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableInt64(v)
			}}
			continue
		case "array(nullable(uint8))":
			converters[i] = ColumnConverter{"array(nullable(uint8))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableUint8(v)
			}}
			continue
		case "array(nullable(uint16))":
			converters[i] = ColumnConverter{"array(nullable(uint16))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableUint16(v)
			}}
			continue
		case "array(nullable(uint32))":
			converters[i] = ColumnConverter{"array(nullable(uint32))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableUint32(v)
			}}
			continue
		case "array(nullable(uint64))":
			converters[i] = ColumnConverter{"array(nullable(uint64))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableUint64(v)
			}}
			continue
		case "array(nullable(float32))":
			converters[i] = ColumnConverter{"array(nullable(float32))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableFloat32(v)
			}}
			continue
		case "array(nullable(float64))":
			converters[i] = ColumnConverter{"array(nullable(float64))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableFloat64(v)
			}}
			continue
		case "array(nullable(string))":
			converters[i] = ColumnConverter{"array(nullable(string))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableString(v)
			}}
			continue
		case "array(nullable(bool))", "array(nullable(boolean))":
			converters[i] = ColumnConverter{"array(nullable(bool))", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayNullableBool(v)
			}}
			continue
		}

		// Handle nested wrappers (nullable, low_cardinality) - may be nested
		for {
			changed := false
			if strings.HasPrefix(dbType, "nullable(") {
				dbType = strings.TrimPrefix(dbType, "nullable(")
				dbType = strings.TrimSuffix(dbType, ")")
				changed = true
			}
			if strings.HasPrefix(dbType, "low_cardinality(") {
				dbType = strings.TrimPrefix(dbType, "low_cardinality(")
				dbType = strings.TrimSuffix(dbType, ")")
				changed = true
			}
			if !changed {
				break
			}
		}

		// Handle precision specifications for datetime and decimal types
		if strings.HasPrefix(dbType, "datetime64(") && strings.HasSuffix(dbType, ")") {
			dbType = "datetime64" // Strip precision, treat as datetime64
		}
		if strings.HasPrefix(dbType, "decimal(") && strings.HasSuffix(dbType, ")") {
			// For decimal(P,S), map to appropriate decimal type based on precision
			if strings.Contains(dbType, ",") {
				dbType = "decimal64" // Default to decimal64 for explicit precision
			} else {
				dbType = "decimal" // For decimal(P) without scale
			}
		}

		// Map database types to converters
		switch dbType {
		// Basic integer types
		case "int8":
			converters[i] = ColumnConverter{"int8", func(v interface{}) (interface{}, error) {
				return cast.ToInt8E(v)
			}}
		case "int16":
			converters[i] = ColumnConverter{"int16", func(v interface{}) (interface{}, error) {
				return cast.ToInt16E(v)
			}}
		case "int32":
			converters[i] = ColumnConverter{"int32", func(v interface{}) (interface{}, error) {
				return cast.ToInt32E(v)
			}}
		case "int64":
			converters[i] = ColumnConverter{"int64", func(v interface{}) (interface{}, error) {
				return cast.ToInt64E(v)
			}}

		// Unsigned integer types
		case "uint8":
			converters[i] = ColumnConverter{"uint8", func(v interface{}) (interface{}, error) {
				return cast.ToUint8E(v)
			}}
		case "uint16":
			converters[i] = ColumnConverter{"uint16", func(v interface{}) (interface{}, error) {
				return cast.ToUint16E(v)
			}}
		case "uint32":
			converters[i] = ColumnConverter{"uint32", func(v interface{}) (interface{}, error) {
				return cast.ToUint32E(v)
			}}
		case "uint64":
			converters[i] = ColumnConverter{"uint64", func(v interface{}) (interface{}, error) {
				return cast.ToUint64E(v)
			}}

		// Float types
		case "float32":
			converters[i] = ColumnConverter{"float32", func(v interface{}) (interface{}, error) {
				return cast.ToFloat32E(v)
			}}
		case "float64":
			converters[i] = ColumnConverter{"float64", func(v interface{}) (interface{}, error) {
				return cast.ToFloat64E(v)
			}}

		// String and boolean types
		case "string":
			converters[i] = ColumnConverter{"string", func(v interface{}) (interface{}, error) {
				return cast.ToStringE(v)
			}}
		case "bool", "boolean":
			converters[i] = ColumnConverter{"bool", func(v interface{}) (interface{}, error) {
				return cast.ToBoolE(v)
			}}

		// Array types
		case "array(string)":
			converters[i] = ColumnConverter{"array(string)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayString(v)
			}}
		case "array(bool)", "array(boolean)":
			converters[i] = ColumnConverter{"array(bool)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayBool(v)
			}}
		case "array(int8)":
			converters[i] = ColumnConverter{"array(int8)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayInt8(v)
			}}
		case "array(int16)":
			converters[i] = ColumnConverter{"array(int16)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayInt16(v)
			}}
		case "array(int32)":
			converters[i] = ColumnConverter{"array(int32)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayInt32(v)
			}}
		case "array(int64)":
			converters[i] = ColumnConverter{"array(int64)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayInt64(v)
			}}
		case "array(uint8)":
			converters[i] = ColumnConverter{"array(uint8)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayUint8(v)
			}}
		case "array(uint16)":
			converters[i] = ColumnConverter{"array(uint16)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayUint16(v)
			}}
		case "array(uint32)":
			converters[i] = ColumnConverter{"array(uint32)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayUint32(v)
			}}
		case "array(uint64)":
			converters[i] = ColumnConverter{"array(uint64)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayUint64(v)
			}}
		case "array(float32)":
			converters[i] = ColumnConverter{"array(float32)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayFloat32(v)
			}}
		case "array(float64)":
			converters[i] = ColumnConverter{"array(float64)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayFloat64(v)
			}}

		// Map types
		case "map(string, string)":
			converters[i] = ColumnConverter{"map(string, string)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringString(v)
			}}
		case "map(string, int32)":
			converters[i] = ColumnConverter{"map(string, int32)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringInt32(v)
			}}
		case "map(string, int64)":
			converters[i] = ColumnConverter{"map(string, int64)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringInt64(v)
			}}
		case "map(string, uint32)":
			converters[i] = ColumnConverter{"map(string, uint32)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringUint32(v)
			}}
		case "map(string, uint64)":
			converters[i] = ColumnConverter{"map(string, uint64)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringUint64(v)
			}}
		case "map(string, float32)":
			converters[i] = ColumnConverter{"map(string, float32)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringFloat32(v)
			}}
		case "map(string, float64)":
			converters[i] = ColumnConverter{"map(string, float64)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringFloat64(v)
			}}
		case "map(string, array(string))":
			converters[i] = ColumnConverter{"map(string, array(string))", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringArrayString(v)
			}}
		case "map(string, array(float32))":
			converters[i] = ColumnConverter{"map(string, array(float32))", func(v interface{}) (interface{}, error) {
				return conn.convertToMapStringArrayFloat32(v)
			}}
		case "map(int32, string)":
			converters[i] = ColumnConverter{"map(int32, string)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapInt32String(v)
			}}
		case "map(int64, string)":
			converters[i] = ColumnConverter{"map(int64, string)", func(v interface{}) (interface{}, error) {
				return conn.convertToMapInt64String(v)
			}}

		// Decimal types for precise calculations
		case "decimal", "decimal32", "decimal64", "decimal128", "decimal256", "numeric", "money":
			converters[i] = ColumnConverter{"decimal", func(v interface{}) (interface{}, error) {
				val, err := decimal.NewFromString(cast.ToString(v))
				return val, err
			}}

		// Date/Time types for temporal data
		case "date", "date32":
			converters[i] = ColumnConverter{"date", func(v interface{}) (interface{}, error) {
				return cast.ToTimeE(v)
			}}
		case "datetime", "datetime64", "timestamp":
			converters[i] = ColumnConverter{"datetime64", func(v interface{}) (interface{}, error) {
				return cast.ToTimeE(v)
			}}

		// UUID type for unique identifiers
		case "uuid":
			converters[i] = ColumnConverter{"uuid", func(v interface{}) (interface{}, error) {
				return cast.ToStringE(v) // UUIDs are typically handled as strings
			}}

		// Array types for analytical data
		case "array(decimal32)", "array(decimal64)":
			converters[i] = ColumnConverter{"array(decimal)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayDecimal(v)
			}}
		case "array(date)":
			converters[i] = ColumnConverter{"array(date)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayDate(v)
			}}
		case "array(datetime64)":
			converters[i] = ColumnConverter{"array(datetime64)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayDateTime64(v)
			}}
		case "array(uuid)":
			converters[i] = ColumnConverter{"array(uuid)", func(v interface{}) (interface{}, error) {
				return conn.convertToArrayUUID(v)
			}}

		default:
			// Fall back to col.Type if DbType is not recognized
			switch {
			case col.Type == iop.DecimalType:
				converters[i] = ColumnConverter{"decimal", func(v interface{}) (interface{}, error) {
					val, err := decimal.NewFromString(cast.ToString(v))
					return val, err
				}}
			case col.Type == iop.SmallIntType:
				converters[i] = ColumnConverter{"smallint", func(v interface{}) (interface{}, error) {
					return cast.ToIntE(v)
				}}
			case col.Type.IsInteger():
				converters[i] = ColumnConverter{"integer", func(v interface{}) (interface{}, error) {
					return cast.ToInt64E(v)
				}}
			case col.Type == iop.FloatType:
				converters[i] = ColumnConverter{"float", func(v interface{}) (interface{}, error) {
					return cast.ToFloat64E(v)
				}}
			}
		}
	}

	return converters
}

// ExecContext runs a sql query with context, returns `error`
func (conn *ProtonConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	err = retryWithBackoff(func() error {
		var execErr error
		result, execErr = conn.BaseConn.ExecContext(ctx, q, args...)
		return execErr
	})

	if err != nil {
		g.Error(err, "Failed to execute query after retries")
	}

	return result, err
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *ProtonConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	fields := cols.Names()
	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	valuesStr := ""
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			values[i] = conn.bindVar(i+1, field, n, c)
			qFields[i] = conn.Self().Quote(field)
		}
		valuesStr += fmt.Sprintf("(%s),", strings.Join(values, ", "))
	}

	if conn.GetProp("http_url") != "" {
		table, _ := ParseTableName(tableName, conn.GetType())
		tableName = table.NameQ()
	}

	settings := ""
	if conn.Idempotent {
		if len(conn.IdempotentPrefix) > 0 {
			settings = fmt.Sprintf(" settings idempotent_id='%s'", conn.IdempotentPrefix)
		} else {
			// Use a default prefix with timestamp if not provided
			defaultPrefix := time.Now().Format("20060102150405")
			settings = fmt.Sprintf(" settings idempotent_id='%s'", defaultPrefix)
		}
	}

	statement := g.R(
		"insert into {table} ({fields}) {settings} values {values}",
		"table", tableName,
		"settings", settings,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	g.Trace("insert statement: "+strings.Split(statement, ") values  ")[0]+")"+" x %d", numRows)
	return statement
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *ProtonConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	// proton does not support upsert with delete
	sqlTempl := `
	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from table({src_table}) src
	`
	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
	)

	return
}

func processProtonInsertRow(columns iop.Columns, row []any) []any {
	for i := range row {
		if columns[i].Type == iop.DecimalType {
			sVal := cast.ToString(row[i])
			if sVal != "" {
				val, err := decimal.NewFromString(sVal)
				if !g.LogError(err, "could not convert value `%s` for Timeplus decimal", sVal) {
					row[i] = val
				}
			} else {
				row[i] = nil
			}
		} else if columns[i].Type == iop.FloatType {
			row[i] = cast.ToFloat64(row[i])
		}
	}
	return row
}

// GetCount returns count of records
func (conn *ProtonConn) GetCount(tableFName string) (uint64, error) {
	// Add another sleep, reason: after insert table we try to getcount directly to ensure no record missing
	// but proton seems not fully ready to get count.
	time.Sleep(countWaitDuration)
	var count uint64

	// Retry logic to handle occasional zero count, likely due to database latency or transactional delays.
	// Temporary workaround while investigating the root cause.
	for attempt := 0; attempt < maxRetries; attempt++ {
		sql := fmt.Sprintf(`select count(*) as cnt from table(%s)`, tableFName)
		data, err := conn.Self().Query(sql)
		if err != nil {
			g.LogError(err, "could not get row number")
			return 0, err
		}

		count = cast.ToUint64(data.Rows[0][0])
		if count > 0 {
			return count, nil
		}

		if attempt < maxRetries-1 {
			g.Debug("Got zero count for %s, retrying in %v (attempt %d/%d)",
				tableFName, countWaitDuration, attempt+1, maxRetries)
			time.Sleep(countWaitDuration)
		}
	}

	// Return 0 after max retries if no valid count is obtained
	return count, nil
}

func (conn *ProtonConn) GetNativeType(col iop.Column) (nativeType string, err error) {
	nativeType, err = conn.BaseConn.GetNativeType(col)

	// remove nullable if part of pk
	if col.IsKeyType(iop.PrimaryKey) && strings.HasPrefix(nativeType, "nullable(") {
		nativeType = strings.TrimPrefix(nativeType, "nullable(")
		nativeType = strings.TrimSuffix(nativeType, ")")
	}

	// special case for _tp_time, Column _tp_time is reserved, expected type is non-nullable datetime64
	if col.Name == "_tp_time" {
		return "datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, LZ4)", nil
	}

	// special case for _tp_sn, Column _tp_sn is reserved, expected type is non-nullable int64
	if col.Name == "_tp_sn" {
		return "int64 CODEC(Delta(8), ZSTD(1))", nil
	}

	if col.DbType != "" {
		dbType := col.DbType

		// Check if the type is nullable
		isNullable := strings.HasPrefix(col.DbType, "nullable(")
		if isNullable {
			// Extract the inner type
			dbType = strings.TrimPrefix(dbType, "nullable(")
			dbType = strings.TrimSuffix(dbType, ")")
		}

		if mappedType, ok := protonDataTypeMap[dbType]; ok {
			if isNullable {
				return "nullable(" + mappedType + ")", nil
			}
			return mappedType, nil
		}

		// Check if the type is low cardinality
		isLowCardinality := strings.HasPrefix(col.DbType, "low_cardinality(")
		if isLowCardinality {
			dbType = strings.TrimPrefix(dbType, "low_cardinality(")
			dbType = strings.TrimSuffix(dbType, ")")
		}

		if mappedType, ok := protonDataTypeMap[dbType]; ok {
			if isLowCardinality {
				return "low_cardinality(" + mappedType + ")", nil
			}
			return mappedType, nil
		}
	}

	return nativeType, err
}

// Array types
func (conn *ProtonConn) convertToArrayString(value interface{}) ([]string, error) {

	if value == "" {
		return []string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayInt8(value interface{}) ([]int8, error) {

	if value == "" {
		return []int8{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int8
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayInt16(value interface{}) ([]int16, error) {

	if value == "" {
		return []int16{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int16
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayInt32(value interface{}) ([]int32, error) {

	if value == "" {
		return []int32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayInt64(value interface{}) ([]int64, error) {

	if value == "" {
		return []int64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayUint8(value interface{}) ([]uint8, error) {

	if value == "" {
		return []uint8{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint8
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayUint16(value interface{}) ([]uint16, error) {

	if value == "" {
		return []uint16{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint16
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayUint32(value interface{}) ([]uint32, error) {

	if value == "" {
		return []uint32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayUint64(value interface{}) ([]uint64, error) {

	if value == "" {
		return []uint64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayFloat32(value interface{}) ([]float32, error) {

	if value == "" {
		return []float32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []float32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayFloat64(value interface{}) ([]float64, error) {

	if value == "" {
		return []float64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []float64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayBool(value interface{}) ([]bool, error) {

	if value == "" {
		return []bool{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []bool
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

// Map types
func (conn *ProtonConn) convertToMapStringUint64(value interface{}) (map[string]uint64, error) {

	if value == "" {
		return map[string]uint64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]uint64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringUint32(value interface{}) (map[string]uint32, error) {

	if value == "" {
		return map[string]uint32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]uint32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringInt32(value interface{}) (map[string]int32, error) {

	if value == "" {
		return map[string]int32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]int32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringInt64(value interface{}) (map[string]int64, error) {

	if value == "" {
		return map[string]int64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]int64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringFloat64(value interface{}) (map[string]float64, error) {

	if value == "" {
		return map[string]float64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]float64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringFloat32(value interface{}) (map[string]float32, error) {

	if value == "" {
		return map[string]float32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]float32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapInt32String(value interface{}) (map[int32]string, error) {

	if value == "" {
		return map[int32]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[int32]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapInt64String(value interface{}) (map[int64]string, error) {

	if value == "" {
		return map[int64]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[int64]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringArrayString(value interface{}) (map[string][]string, error) {

	if value == "" {
		return map[string][]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string][]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringArrayFloat32(value interface{}) (map[string][]float32, error) {

	if value == "" {
		return map[string][]float32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string][]float32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringString(value interface{}) (map[string]string, error) {

	if value == "" {
		return map[string]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

// Decimal/Date/Time converter functions for analytical data

func (conn *ProtonConn) convertToArrayDecimal(value interface{}) ([]decimal.Decimal, error) {
	if value == "" {
		return []decimal.Decimal{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var stringResults []string
	err := json.Unmarshal([]byte(str), &stringResults)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	results := make([]decimal.Decimal, len(stringResults))
	for i, s := range stringResults {
		dec, err := decimal.NewFromString(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse decimal %s: %v", s, err)
		}
		results[i] = dec
	}

	return results, nil
}

func (conn *ProtonConn) convertToArrayDate(value interface{}) ([]time.Time, error) {
	if value == "" {
		return []time.Time{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var stringResults []string
	err := json.Unmarshal([]byte(str), &stringResults)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	results := make([]time.Time, len(stringResults))
	for i, s := range stringResults {
		t, err := cast.ToTimeE(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date %s: %v", s, err)
		}
		results[i] = t
	}

	return results, nil
}

func (conn *ProtonConn) convertToArrayDateTime64(value interface{}) ([]time.Time, error) {
	if value == "" {
		return []time.Time{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var stringResults []string
	err := json.Unmarshal([]byte(str), &stringResults)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	results := make([]time.Time, len(stringResults))
	for i, s := range stringResults {
		t, err := cast.ToTimeE(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse datetime %s: %v", s, err)
		}
		results[i] = t
	}

	return results, nil
}

func (conn *ProtonConn) convertToArrayUUID(value interface{}) ([]string, error) {
	if value == "" {
		return []string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

// Array with nullable elements - these return pointer slices as required by Proton driver

func (conn *ProtonConn) convertToArrayNullableInt8(value interface{}) ([]*int8, error) {
	if value == "" {
		return []*int8{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*int8, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToInt8(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableInt16(value interface{}) ([]*int16, error) {
	if value == "" {
		return []*int16{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*int16, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToInt16(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableInt32(value interface{}) ([]*int32, error) {
	if value == "" {
		return []*int32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*int32, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToInt32(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableInt64(value interface{}) ([]*int64, error) {
	if value == "" {
		return []*int64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*int64, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToInt64(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableUint8(value interface{}) ([]*uint8, error) {
	if value == "" {
		return []*uint8{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*uint8, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToUint8(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableUint16(value interface{}) ([]*uint16, error) {
	if value == "" {
		return []*uint16{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*uint16, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToUint16(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableUint32(value interface{}) ([]*uint32, error) {
	if value == "" {
		return []*uint32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*uint32, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToUint32(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableUint64(value interface{}) ([]*uint64, error) {
	if value == "" {
		return []*uint64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*uint64, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToUint64(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableFloat32(value interface{}) ([]*float32, error) {
	if value == "" {
		return []*float32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*float32, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToFloat32(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableFloat64(value interface{}) ([]*float64, error) {
	if value == "" {
		return []*float64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*float64, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToFloat64(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableString(value interface{}) ([]*string, error) {
	if value == "" {
		return []*string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*string, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToString(v)
			result[i] = &val
		}
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayNullableBool(value interface{}) ([]*bool, error) {
	if value == "" {
		return []*bool{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var rawResult []interface{}
	err := json.Unmarshal([]byte(str), &rawResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	result := make([]*bool, len(rawResult))
	for i, v := range rawResult {
		if v == nil {
			result[i] = nil
		} else {
			val := cast.ToBool(v)
			result[i] = &val
		}
	}

	return result, nil
}
