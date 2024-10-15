package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

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

// ProtonConn is a Proton connection
type ProtonConn struct {
	BaseConn
	URL              string
	Idempotent       bool
	IdempotentPrefix string
	ProtonConn       proton.Conn
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
func retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; i < attempts; i++ {
		err = f()
		if err == nil {
			return nil
		}

		g.Error("Attempt %d failed: %v", i+1, err)

		if i < attempts-1 { // don't sleep after the last attempt
			g.Info("Sleeping for %v before next attempt", sleep)
			time.Sleep(sleep)
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
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
		err = retry(maxRetries, retryDelay, func() error {
			return conn.processBatch(tableFName, table, batch, columns, batchCount, &count, ds)
		})

		if err != nil {
			g.Error("Failed to process batch %d after %d retries: %v", batchCount, maxRetries, err)
			return count, g.Error(err, "could not copy data after retries")
		}
	}

	g.Info("Bulk import completed: %d batches, %d rows", batchCount, count)

	ds.SetEmpty()

	g.Debug("%d ROWS COPIED", count)
	return count, nil
}

// processBatch handles the processing of a single batch
func (conn *ProtonConn) processBatch(tableFName string, table Table, batch *iop.Batch, columns iop.Columns, batchCount int, count *uint64, ds *iop.Datastream) error {
	// COPY needs a transaction
	if conn.Tx() == nil {
		err := conn.Begin(&sql.TxOptions{Isolation: sql.LevelDefault})
		if err != nil {
			return g.Error(err, "could not begin transaction")
		}
		defer conn.Rollback()
	}

	insFields, err := conn.ValidateColumnNames(columns, batch.Columns.Names(), true)
	if err != nil {
		return g.Error(err, "columns mismatch")
	}

	// Enable idempotent support with a specific prefix
	idPrefix := fmt.Sprintf("%d_batch_%s", batchCount, time.Now().Format("20060102150405"))
	conn.SetIdempotent(true, idPrefix)

	insertStatement := conn.GenerateInsertStatement(
		table.FullName(),
		insFields,
		1,
	)

	batched, err := conn.ProtonConn.PrepareBatch(ds.Context.Ctx, insertStatement)
	if err != nil {
		return g.Error(err, "could not prepare statement for table: %s, statement: %s", table.FullName(), insertStatement)
	}

	decimalCols := []int{}
	intCols := []int{}
	int64Cols := []int{}
	floatCols := []int{}
	for i, col := range batch.Columns {
		switch {
		case col.Type == iop.DecimalType:
			decimalCols = append(decimalCols, i)
		case col.Type == iop.SmallIntType:
			intCols = append(intCols, i)
		case col.Type.IsInteger():
			int64Cols = append(int64Cols, i)
		case col.Type == iop.FloatType:
			floatCols = append(floatCols, i)
		}
	}

	// Counter for successfully inserts within this batch
	var internalCount uint64
	for row := range batch.Rows {
		var eG g.ErrorGroup

		// set decimals correctly
		for _, colI := range decimalCols {
			if row[colI] != nil {
				val, err := decimal.NewFromString(cast.ToString(row[colI]))
				if err == nil {
					row[colI] = val
				}
				eG.Capture(err)
			}
		}

		// set Int32 correctly
		for _, colI := range intCols {
			if row[colI] != nil {
				row[colI], err = cast.ToIntE(row[colI])
				eG.Capture(err)
			}
		}

		// set Int64 correctly
		for _, colI := range int64Cols {
			if row[colI] != nil {
				row[colI], err = cast.ToInt64E(row[colI])
				eG.Capture(err)
			}
		}

		// set Float64 correctly
		for _, colI := range floatCols {
			if row[colI] != nil {
				row[colI], err = cast.ToFloat64E(row[colI])
				eG.Capture(err)
			}
		}

		if err = eG.Err(); err != nil {
			err = g.Error(err, "could not convert value for COPY into table %s", tableFName)
			ds.Context.CaptureErr(err)
			return err
		}

		// Do insert
		ds.Context.Lock()
		err = batched.Append(row...)
		ds.Context.Unlock()
		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "could not insert into table %s, row: %#v", tableFName, row))
			return g.Error(err, "could not execute statement")
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

	*count += internalCount
	return nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *ProtonConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	retries := 0

	for {
		result, err = conn.BaseConn.ExecContext(ctx, q, args...)
		if err == nil {
			return
		}

		g.Warn("Error executing query (attempt %d): %v", retries+1, err)

		retries++
		if retries >= maxRetries {
			g.Error("Max retries reached. Last error: %v", err)
			return
		}

		g.Info("Sleeping for %v(sec) before retry", retryDelay.Seconds())
		time.Sleep(retryDelay)
	}
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
	// wait for a while before getting count, otherwise newly added row can be 0
	time.Sleep(countWaitDuration)
	sql := fmt.Sprintf(`select count(*) as cnt from table(%s)`, tableFName)
	data, err := conn.Self().Query(sql)
	if err != nil {
		g.LogError(err, "could not get row number")
		return 0, err
	}
	return cast.ToUint64(data.Rows[0][0]), nil
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

	return nativeType, err
}
