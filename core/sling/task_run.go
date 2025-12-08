package sling

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/cenkalti/backoff/v4"
	"github.com/slingdata-io/sling-cli/core"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// connPool a way to cache connections to that they don't have to reconnect
// for each replication steps
var connPool = map[string]database.Connection{}

var start time.Time
var slingLoadedAtColumn = "_sling_loaded_at"
var slingStreamURLColumn = "_sling_stream_url"
var slingRowNumColumn = "_sling_row_num"
var slingRowIDColumn = "_sling_row_id"

func init() {
	// we need a webserver to get the pprof webserver
	if cast.ToBool(os.Getenv("SLING_PPROF")) {
		go func() {
			g.Debug("Starting pprof webserver @ localhost:6060")
			g.LogError(http.ListenAndServe("localhost:6060", nil))
		}()
	}
}

// Execute runs a Sling task.
// This may be a file/db to file/db transfer
func (t *TaskExecution) Execute() error {

	// Force SLING_PROCESS_BW to false for Proton issue https://github.com/flarco/g/issues/1
	if t.Config.Target.Type == dbio.TypeDbProton || t.Config.Source.Type == dbio.TypeDbProton {
		g.Debug("Force SLING_PROCESS_BW to false for timeplus database")
		os.Setenv("SLING_PROCESS_BW", "false")
	}

	done := make(chan struct{})
	now := time.Now()
	t.StartTime = &now
	t.lastIncrement = now

	if t.Context == nil {
		ctx := g.NewContext(context.Background())
		t.Context = &ctx
	}

	// get stats of process at beginning
	t.ProcStatsStart = g.GetProcStats(os.Getpid())

	// set defaults
	t.Config.SetDefault()

	// print for debugging
	g.Trace("using Config:\n%s", g.Pretty(t.Config))
	env.SetTelVal("stage", "2 - task-execution")

	go func() {
		defer close(done)
		defer t.PBar.Finish()

		// recover from panic
		defer func() {
			if r := recover(); r != nil {
				t.Err = g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			}
		}()

		t.Status = ExecStatusRunning

		if t.Err != nil {
			return
		}

		// update into store
		StoreUpdate(t)

		g.DebugLow("Sling version: %s (%s %s)", core.Version, runtime.GOOS, runtime.GOARCH)
		g.DebugLow("type is %s", t.Type)
		g.Debug("using: %s", g.Marshal(g.M("mode", t.Config.Mode, "columns", t.Config.Target.Columns, "transforms", t.Config.Transforms)))
		g.Debug("using source options: %s", g.Marshal(t.Config.Source.Options))
		g.Debug("using target options: %s", g.Marshal(t.Config.Target.Options))

		switch t.Type {
		case DbSQL:
			t.Err = t.runDbSQL()
		case FileToDB:
			t.Err = t.runFileToDB()
		case DbToDb:
			t.Err = t.runDbToDb()
		case DbToFile:
			t.Err = t.runDbToFile()
		case FileToFile:
			t.Err = t.runFileToFile()
		default:
			t.SetProgress("task execution configuration is invalid")
			t.Err = g.Error("Cannot Execute. Task Type is not specified")
		}

		// update into store
		StoreUpdate(t)
	}()

	select {
	case <-done:
		t.Cleanup()
	case <-t.Context.Ctx.Done():
		go t.Cleanup()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
		if t.Err == nil {
			t.Err = g.Error("Execution interrupted")
		}
	}

	if t.Err == nil {
		t.SetProgress("execution succeeded")
		t.Status = ExecStatusSuccess
	} else {
		t.SetProgress("execution failed")
		t.Status = ExecStatusError
		if err := t.df.Context.Err(); err != nil && err.Error() != t.Err.Error() {
			eG := g.ErrorGroup{}
			eG.Add(err)
			eG.Add(t.Err)
			t.Err = g.Error(eG.Err())
		} else {
			t.Err = g.Error(t.Err)
		}
	}

	now2 := time.Now()
	t.EndTime = &now2

	return t.Err
}

func (t *TaskExecution) getSrcDBConn(ctx context.Context) (conn database.Connection, err error) {

	// sets metadata
	metadata := t.setGetMetadata()

	options := t.getOptionsMap()
	options["METADATA"] = g.Marshal(metadata)

	srcProps := append(
		g.MapToKVArr(t.Config.SrcConn.DataS()),
		g.MapToKVArr(g.ToMapString(options))...,
	)

	conn, err = database.NewConnContext(ctx, t.Config.SrcConn.URL(), srcProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	// look for conn in cache
	if c, ok := connPool[t.Config.SrcConn.Hash()]; ok {
		// update properties
		c.Base().ReplaceProps(conn.Props())
		return c, nil
	}

	// cache connection is using replication from CLI
	if t.isUsingPool() {
		connPool[t.Config.SrcConn.Hash()] = conn
	}

	err = conn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to source connection")
		return
	}

	// set read_only if sqlite / duckdb since it's a source
	if g.In(conn.GetType(), dbio.TypeDbSQLite, dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck) {
		conn.SetProp("read_only", "true")
	}

	return
}

func (t *TaskExecution) getTgtDBConn(ctx context.Context) (conn database.Connection, err error) {

	options := g.M()
	g.Unmarshal(g.Marshal(t.Config.Target.Options), &options)
	tgtProps := append(
		g.MapToKVArr(t.Config.TgtConn.DataS()), g.MapToKVArr(g.ToMapString(options))...,
	)

	// Connection context should be different than task context
	conn, err = database.NewConnContext(ctx, t.Config.TgtConn.URL(), tgtProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	// look for conn in cache
	if c, ok := connPool[t.Config.TgtConn.Hash()]; ok {
		// update properties
		c.Base().ReplaceProps(conn.Props())
		return c, nil
	}

	// cache connection is using replication from CLI
	if t.isUsingPool() {
		connPool[t.Config.TgtConn.Hash()] = conn
	}

	err = conn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to target connection")
		return
	}

	// set bulk
	if val := t.Config.Target.Options.UseBulk; val != nil && !*val {
		conn.SetProp("use_bulk", "false")
		conn.SetProp("allow_bulk_import", "false")
	}
	return
}

func (t *TaskExecution) runDbSQL() (err error) {

	start = time.Now()

	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	t.SetProgress("connecting to target database (%s)", tgtConn.GetType())
	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	if !t.isUsingPool() {
		defer tgtConn.Close()
	}

	t.SetProgress("executing sql on target database")
	result, err := tgtConn.Exec(t.Config.Target.Object)
	if err != nil {
		err = g.Error(err, "Could not complete sql execution on %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	rowAffCnt, err := result.RowsAffected()
	if err == nil {
		t.SetProgress("%d rows affected", rowAffCnt)
	}

	return
}

func (t *TaskExecution) runDbToFile() (err error) {

	start = time.Now()

	srcConn, err := t.getSrcDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	t.SetProgress("connecting to source database (%s)", srcConn.GetType())
	err = srcConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.SrcConn.Info().Name, srcConn.GetType())
		return
	}

	if !t.isUsingPool() {
		defer srcConn.Close()
	}

	t.SetProgress("reading from source database")
	defer t.Cleanup()
	t.df, err = t.ReadFromDB(t.Config, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer t.df.Close()

	if t.Config.Options.StdOut {
		t.SetProgress("writing to target stream (stdout)")
	} else {
		t.SetProgress("writing to target file system (%s)", t.Config.TgtConn.Type)
	}
	cnt, err := t.WriteToFile(t.Config, t.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	t.SetProgress("wrote %d rows [%s r/s] to %s", cnt, getRate(cnt), t.getTargetObjectValue())

	err = t.df.Err()
	return

}

func (t *TaskExecution) runFolderToDB() (err error) {
	/*
		This will take a URL as a folder path
		1. list the files/folders in it (not recursive)
		2a. run runFileToDB for each of the files, naming the target table respectively
		2b. OR run runFileToDB for each of the files, to the same target able, assume each file has same structure
		3. keep list of file inserted in Job.Settings (view handleExecutionHeartbeat in server_ws.go).

	*/
	return
}

func (t *TaskExecution) runFileToDB() (err error) {

	start = time.Now()

	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	t.SetProgress("connecting to target database (%s)", tgtConn.GetType())
	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	if !t.isUsingPool() {
		t.AddCleanupTaskLast(func() { tgtConn.Close() })
	}

	if t.Config.Target.Type == dbio.TypeDbProton && t.Config.Mode == IncrementalMode {
		t.Config.Target.Object = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Object)

		targetTable, err := database.ParseTableName(t.Config.Target.Object, tgtConn.GetType())
		if err != nil {
			return g.Error(err, "could not parse target table")
		}

		existed, err := database.TableExists(tgtConn, targetTable.FullName())
		if err != nil {
			return g.Error(err, "could not check if final table exists in incremental mode")
		}
		if !existed {
			err = g.Error("final table %s not found in incremental mode, please create table %s first", t.Config.Target.Object, t.Config.Target.Object)
			return err
		}

		if targetTable.Columns, err = tgtConn.GetSQLColumns(targetTable); err != nil {
			return g.Error(err, "could not get table columns, when write to timeplusd database in incremental mode, final table %s need created first", targetTable.FullName())
		}

		t.Config.Target.Columns = targetTable.Columns
	}

	// check if table exists by getting target columns
	// only pull if ignore_existing is specified (don't need columns yet otherwise)
	if t.Config.IgnoreExisting() {
		if cols, _ := pullTargetTableColumns(t.Config, tgtConn, false); len(cols) > 0 {
			g.Debug("not writing since table exists at %s (ignore_existing=true)", t.Config.Target.Object)
			return nil
		}
	}

	if t.isIncrementalWithUpdateKey() {
		// Normalize shorthand for loaded_at before deciding whether to fetch the checkpoint
		if t.Config.Source.UpdateKey == "." {
			t.Config.Source.UpdateKey = slingLoadedAtColumn
		}

		// For file->Proton restore use-cases we may want to skip the checkpoint
		// (max(update_key)) so that older rows can be re-inserted. In that case
		// we honor the Config flag and do not call getIncrementalValue.
		if t.Config.SkipIncrementalCheckpoint() && t.Config.TgtConn.Type == dbio.TypeDbProton && t.Config.SrcConn.Type.Kind() == dbio.KindFile {
			g.Debug("skipping incremental checkpoint for file->proton (skip_incremental_checkpoint=true)")
		} else {
			t.SetProgress("getting checkpoint value")
			template, _ := dbio.TypeDbDuckDb.Template()
			if err = getIncrementalValue(t.Config, tgtConn, template.Variable); err != nil {
				err = g.Error(err, "Could not get incremental value")
				return err
			}
		}
	}

	if t.Config.Options.StdIn && t.Config.SrcConn.Type.IsUnknown() {
		t.SetProgress("reading from stream (stdin)")
	} else {
		t.SetProgress("reading from source file system (%s)", t.Config.SrcConn.Type)
	}
	t.df, err = t.ReadFromFile(t.Config)
	if err != nil {
		if strings.Contains(err.Error(), "Provided 0 files") {
			if t.isIncrementalWithUpdateKey() && t.Config.IncrementalVal != nil {
				t.SetProgress("no new files found since latest timestamp (%s)", time.Unix(cast.ToInt64(t.Config.IncrementalValStr), 0))
			} else {
				t.SetProgress("no files found")
			}
			return nil
		} else if len(t.df.Streams) == 1 && t.df.Streams[0].IsClosed() {
			return nil
		}
		err = g.Error(err, "could not read from file")
		return
	}
	defer t.df.Close()

	// set schema if needed
	t.Config.Target.Object = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Object)
	t.Config.Target.Options.TableTmp = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Options.TableTmp)

	t.SetProgress("writing to target database [mode: %s]", t.Config.Mode)
	defer t.Cleanup()
	cnt, err := t.WriteToDb(t.Config, t.df, tgtConn)
	if err != nil {
		err = g.Error(err, "could not write to database")
		return
	}

	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("inserted %d rows into %s in %d secs [%s r/s]", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt))

	if err != nil {
		err = g.Error(t.df.Err(), "error in transfer")
	}
	return
}

func (t *TaskExecution) runFileToFile() (err error) {

	start = time.Now()

	if t.Config.Options.StdIn && t.Config.SrcConn.Type.IsUnknown() {
		t.SetProgress("reading from stream (stdin)")
	} else {
		t.SetProgress("reading from source file system (%s)", t.Config.SrcConn.Type)
	}
	t.df, err = t.ReadFromFile(t.Config)
	if err != nil {
		if strings.Contains(err.Error(), "Provided 0 files") {
			if t.isIncrementalWithUpdateKey() && t.Config.IncrementalVal != nil {
				t.SetProgress("no new files found since latest timestamp (%s)", time.Unix(cast.ToInt64(t.Config.IncrementalValStr), 0))
			} else {
				t.SetProgress("no files found")
			}
			return nil
		} else if len(t.df.Streams) == 1 && t.df.Streams[0].IsClosed() {
			return nil
		}
		err = g.Error(err, "Could not ReadFromFile")
		return
	}
	defer t.df.Close()

	if t.Config.Options.StdOut {
		t.SetProgress("writing to target stream (stdout)")
	} else {
		t.SetProgress("writing to target file system (%s)", t.Config.TgtConn.Type)
	}
	defer t.Cleanup()
	cnt, err := t.WriteToFile(t.Config, t.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("wrote %d rows to %s in %d secs [%s r/s]", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt))

	if t.df.Err() != nil {
		err = g.Error(t.df.Err(), "Error in runFileToFile")
	}
	return
}

func (t *TaskExecution) runDbToDb() (err error) {
	start = time.Now()
	if t.Config.Mode == Mode("") {
		t.Config.Mode = FullRefreshMode
	}

	// Initiate connections
	srcConn, err := t.getSrcDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	t.SetProgress("connecting to source database (%s)", srcConn.GetType())
	err = srcConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.SrcConn.Info().Name, srcConn.GetType())
		return
	}

	t.SetProgress("connecting to target database (%s)", tgtConn.GetType())
	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	if !t.isUsingPool() {
		t.AddCleanupTaskLast(func() { srcConn.Close() })
		t.AddCleanupTaskLast(func() { tgtConn.Close() })
	}

	// set schema if needed
	t.Config.Target.Object = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Object)
	t.Config.Target.Options.TableTmp = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Options.TableTmp)

	// check if table exists by getting target columns
	if cols, _ := pullTargetTableColumns(t.Config, tgtConn, false); len(cols) > 0 {
		if t.Config.IgnoreExisting() {
			g.Debug("not writing since table exists at %s (ignore_existing=true)", t.Config.Target.Object)
			return nil
		}
	}

	// get watermark
	if t.isIncrementalWithUpdateKey() {
		if t.Config.SkipIncrementalCheckpoint() && t.Config.TgtConn.Type == dbio.TypeDbProton && t.Config.SrcConn.Type == dbio.TypeDbProton {
			g.Debug("skipping incremental checkpoint for proton->proton (skip_incremental_checkpoint=true)")
		} else {
			t.SetProgress("getting checkpoint value")
			if err = getIncrementalValue(t.Config, tgtConn, srcConn.Template().Variable); err != nil {
				err = g.Error(err, "Could not get incremental value")
				return err
			}
		}
	}

	if srcConn.GetType() == dbio.TypeDbProton && tgtConn.GetType() == dbio.TypeDbProton {
		return t.runProtonToProton(srcConn, tgtConn)
	}

	t.SetProgress("reading from source database")
	t.df, err = t.ReadFromDB(t.Config, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer t.df.Close()

	// to DirectLoad if possible
	if t.df.FsURL != "" {
		data := g.M("url", t.df.FsURL)
		for k, v := range srcConn.Props() {
			data[k] = v
		}
		t.Config.Source.Data["SOURCE_FILE"] = g.M("data", data)
	}

	t.SetProgress("writing to target database [mode: %s]", t.Config.Mode)
	defer t.Cleanup()
	cnt, err := t.WriteToDb(t.Config, t.df, tgtConn)
	if err != nil {
		err = g.Error(err, "Could not WriteToDb")
		return
	}

	bytesStr := ""
	if val := t.GetBytesString(); val != "" {
		bytesStr = "[" + val + "]"
	}
	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("inserted %d rows into %s in %d secs [%s r/s] %s", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt), bytesStr)

	if t.df.Err() != nil {
		err = g.Error(t.df.Err(), "Error running runDbToDb")
	}
	return
}

func CreateTempFile(fileName string) (*os.File, error) {
	// First try /app directory
	appFolder := "/app"

	// Check if /app exists and we have write permissions
	if info, err := os.Stat(appFolder); err == nil && info.IsDir() {
		// Try to create a test file to verify write permissions
		testPath := filepath.Join(appFolder, ".write_test")
		if testFile, err := os.Create(testPath); err == nil {
			testFile.Close()
			os.Remove(testPath) // Clean up test file

			// /app exists and is writable, create file there
			tempFilePath := filepath.Join(appFolder, fileName)
			if file, err := os.Create(tempFilePath); err == nil {
				return file, nil
			} else {
				return nil, g.Error("failed to create file in /app: %v", err)
			}
		}
	}

	// Fallback to current directory
	localFolder := "sling_transfer"
	if err := os.MkdirAll(localFolder, 0755); err != nil {
		return nil, g.Error("failed to create local directory: %v", err)
	}

	tempFilePath := filepath.Join(localFolder, fileName)
	file, err := os.Create(tempFilePath)
	if err != nil {
		return nil, g.Error("failed to create file in local directory: %v", err)
	}

	return file, nil
}

func (t *TaskExecution) createIntermediateConfig() *Config {
	intermediateConfig := *t.Config // Create a copy of the original config

	// Set up the intermediate file
	timestamp := time.Now().Format("20060102_150405")
	sourceTable := t.Config.Source.Stream
	targetTable := t.Config.Target.Object
	tempFileName := fmt.Sprintf("timeplus_database_%s_%s_to_%s.csv", timestamp, sourceTable, targetTable)

	// Create or overwrite the file (this is an intermediate file, no need to check if it exists)
	tempFile, err := CreateTempFile(tempFileName)
	if err != nil {
		g.Error(err, "Could not create temporary file '%s' when transferring from %s to %s", tempFileName, t.Config.Source.Stream, t.Config.Target.Object)
		return nil
	}

	intermediateConfig.Target = Target{
		Conn:   "LOCAL",
		Type:   "file",
		Object: tempFile.Name(),
		Options: &TargetOptions{
			MaxDecimals: g.Int(11),
			Format:      dbio.FileTypeCsv,
			Delimiter:   "~", // Use ~ as delimiter for safety
			Header:      g.Bool(true),
			Concurrency: 7,
			UseBulk:     g.Bool(true),
		},
		Data: map[string]interface{}{
			"type": "file",
			"url":  "file://" + tempFile.Name(),
		},
	}

	intermediateConfig.TgtConn = connection.Connection{
		Name: "LOCAL",
		Type: "file",
		Data: map[string]interface{}{
			"type": "file",
			"url":  "file://" + tempFile.Name(),
		},
		File: &filesys.LocalFileSysClient{},
	}

	return &intermediateConfig
}

func (t *TaskExecution) runProtonToProton(srcConn, tgtConn database.Connection) (err error) {
	if t.Config.Target.Type == dbio.TypeDbProton && t.Config.Mode == IncrementalMode {
		existed, err := database.TableExists(tgtConn, t.Config.Target.Object)
		if err != nil {
			return g.Error(err, "could not check if final table exists in incremental mode")
		}
		if !existed {
			err = g.Error("final table %s not found in incremental mode, please create table %s first", t.Config.Target.Object, t.Config.Target.Object)
			return err
		}
	}

	start := time.Now()
	maxRetries := 3
	retryDelay := time.Second * 5

	// Set up interrupt handling
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interruptChan)

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(t.Context.Ctx)
	defer cancel()

	// Step 1: Create intermediate config
	t.SetProgress("Creating intermediate configuration")
	intermediateConfig := t.createIntermediateConfig()
	if intermediateConfig == nil {
		err = g.Error("Failed to create intermediate config")
		return
	}

	cleanup := func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			if intermediateConfig != nil && intermediateConfig.Target.Object != "" {
				os.Remove(intermediateConfig.Target.Object)
			}
		} else {
			g.Debug("keeping intermediate file %s", intermediateConfig.Target.Object)
		}
	}
	defer cleanup()

	// Function to handle interrupts
	go func() {
		select {
		case <-interruptChan:
			t.SetProgress("Interrupt received, cleaning up and exiting")
			cancel()
			cleanup()
			os.Exit(1)
		case <-ctx.Done():
			return
		}
	}()

	// Retry function with cleanup
	retryWithCleanup := func(operation func() error) error {
		for attempt := 1; attempt <= maxRetries; attempt++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				err := operation()
				if err == nil {
					return nil
				}
				if attempt == maxRetries {
					return err
				}
				t.SetProgress("Attempt %d failed, retrying in %v", attempt, retryDelay)
				cleanup()
				time.Sleep(retryDelay)
			}
		}
		return g.Error("Max retries reached")
	}

	// Step 2: Run DbToFile with retries
	t.SetProgress("Exporting data from source Proton database")
	// t.PBar.Start()
	originalConfig := t.Config
	t.Config = intermediateConfig

	g.Debug("proton to proton first stage: writedbtofile using source options: %s", g.Marshal(t.Config.Source.Options))
	g.Debug("proton to proton first stage: writedbtofile using target options: %s", g.Marshal(t.Config.Target.Options))
	err = retryWithCleanup(func() error {
		return t.runDbToFile()
	})
	t.Config = originalConfig // Restore original config
	if err != nil {
		err = g.Error(err, "Failed to export data from source Proton database")
		return
	}

	// Check if the file is empty
	t.SetProgress("Checking exported data")
	fileInfo, err := os.Stat(intermediateConfig.Target.Object)
	if err != nil {
		err = g.Error(err, "Failed to get file info for intermediate file")
		return
	}
	if fileInfo.Size() == 0 {
		t.SetProgress("No new data to transfer")
		return nil
	}

	// Step 3: Run FileToDB with retries
	t.SetProgress("Preparing to import data to target Proton database")
	originalSource := t.Config.Source
	originalSrcConn := t.Config.SrcConn

	csvFormat := dbio.FileTypeCsv
	delimiter := "~"
	header := true

	t.Config.Source = Source{
		Conn:   "LOCAL",
		Type:   "file",
		Stream: intermediateConfig.Target.Object,
		Options: &SourceOptions{
			MaxDecimals: g.Int(11),
			Format:      &csvFormat,
			Delimiter:   delimiter,
			Header:      &header,
		},
		Data: intermediateConfig.Target.Data,
	}
	t.Config.SrcConn = intermediateConfig.TgtConn

	t.SetProgress("Importing data to target Proton database")

	g.Debug("proton to proton second stage: filetodb using source options: %s", g.Marshal(t.Config.Source.Options))
	g.Debug("proton to proton second stage: filetodb using target options: %s", g.Marshal(t.Config.Target.Options))
	err = retryWithBackoff(func() error {
		return t.runFileToDB()
	})

	// Restore original config
	t.Config.Source = originalSource
	t.Config.SrcConn = originalSrcConn
	if err != nil {
		err = g.Error(err, "Failed to import data to target Proton database after %d retries", maxRetries)
		return
	}

	elapsed := int(time.Since(start).Seconds())
	cnt := t.df.Count()
	t.SetProgress("Transferred %d rows between Proton databases in %d secs [%s r/s]", cnt, elapsed, getRate(cnt))

	return nil
}

func retryWithBackoff(operation func() error) error {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 5 * time.Minute // Set a maximum total retry time

	return backoff.RetryNotify(operation, b, func(err error, duration time.Duration) {
		g.Warn("Operation failed, retrying in %v: %v", duration, err)
	})
}
