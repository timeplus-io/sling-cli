package filesys

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gobwas/glob"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"

	"github.com/dustin/go-humanize"
	"github.com/slingdata-io/sling-cli/core/env"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/spf13/cast"
)

var recursiveLimit = cast.ToInt(os.Getenv("SLING_RECURSIVE_LIMIT"))

// FileSysClient is a client to a file systems
// such as local, s3, hdfs, azure storage, google cloud storage
type FileSysClient interface {
	Self() FileSysClient
	Init(ctx context.Context) (err error)
	Close() (err error)
	Client() *BaseFileSysClient
	Context() (context *g.Context)
	FsType() dbio.Type
	GetReader(path string) (reader io.Reader, err error)
	GetReaders(paths ...string) (readers []io.Reader, err error)
	GetDatastream(path string, cfg ...iop.FileStreamConfig) (ds *iop.Datastream, err error)
	GetWriter(path string) (writer io.Writer, err error)
	Buckets() (paths []string, err error)
	List(path string) (paths FileNodes, err error)
	ListRecursive(path string) (paths FileNodes, err error)
	Write(path string, reader io.Reader) (bw int64, err error)
	Prefix(suffix ...string) string
	ReadDataflow(url string, cfg ...iop.FileStreamConfig) (df *iop.Dataflow, err error)
	WriteDataflowReady(df *iop.Dataflow, url string, fileReadyChn chan FileReady, sc *iop.StreamConfig) (bw int64, err error)
	GetProp(key string, keys ...string) (val string)
	SetProp(key string, val string)
	MkdirAll(path string) (err error)
	GetPath(uri string) (path string, err error)
	Query(uri, sql string) (data iop.Dataset, err error)

	delete(path string) (err error)
	setDf(df *iop.Dataflow)
}

// NewFileSysClient create a file system client
// such as local, s3, azure storage, google cloud storage
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClient(fst dbio.Type, props ...string) (fsClient FileSysClient, err error) {
	return NewFileSysClientContext(context.Background(), fst, props...)
}

// NewFileSysClientContext create a file system client with context
// such as local, s3, azure storage, google cloud storage
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientContext(ctx context.Context, fst dbio.Type, props ...string) (fsClient FileSysClient, err error) {
	concurrencyLimit := runtime.NumCPU()
	if os.Getenv("CONCURRENCY_LIMIT") != "" {
		concurrencyLimit = cast.ToInt(os.Getenv("CONCURRENCY_LIMIT"))
	}

	switch fst {
	case dbio.TypeFileLocal:
		fsClient = &LocalFileSysClient{}
		concurrencyLimit = 20
	case dbio.TypeFileS3:
		fsClient = &S3FileSysClient{}
		//fsClient = &S3cFileSysClient{}
		//fsClient.Client().fsType = S3cFileSys
	case dbio.TypeFileFtp:
		fsClient = &FtpFileSysClient{}
		concurrencyLimit = 1 // can only write 1 file at a time
	case dbio.TypeFileSftp:
		fsClient = &SftpFileSysClient{}
	// case HDFSFileSys:
	// 	fsClient = fsClient
	case dbio.TypeFileAzure:
		fsClient = &AzureFileSysClient{}
	case dbio.TypeFileGoogle:
		fsClient = &GoogleFileSysClient{}
	case dbio.TypeFileHTTP:
		fsClient = &HTTPFileSysClient{}
	default:
		err = g.Error("Unrecognized File System")
		return
	}

	fsClient.Client().fsType = fst
	fsClient.Client().context = g.NewContext(ctx)

	// set properties
	for k, v := range g.KVArrToMap(props...) {
		fsClient.SetProp(k, v)
	}

	if fsClient.GetProp("CONCURRENCY_LIMIT") != "" {
		concurrencyLimit = cast.ToInt(fsClient.GetProp("CONCURRENCY_LIMIT"))
	}

	for k, v := range env.Vars() {
		if fsClient.GetProp(k) == "" {
			fsClient.SetProp(k, v)
		}
	}

	// Init Limit
	err = fsClient.Init(ctx)
	if err != nil {
		err = g.Error(err, "Error initiating File Sys Client")
	}
	fsClient.Context().SetConcurrencyLimit(concurrencyLimit)
	fsClient.SetProp("sling_conn_id", g.RandSuffix(g.F("conn-%s-", fst), 3))

	if !cast.ToBool(fsClient.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, fst, fsClient.GetProp("sling_conn_id"))
	}

	return
}

// NewFileSysClientFromURL returns the proper fs client for the given path
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientFromURL(url string, props ...string) (fsClient FileSysClient, err error) {
	return NewFileSysClientFromURLContext(context.Background(), url, props...)
}

// NewFileSysClientFromURLContext returns the proper fs client for the given path with context
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientFromURLContext(ctx context.Context, url string, props ...string) (fsClient FileSysClient, err error) {
	switch {
	case strings.HasPrefix(url, "s3://"):
		props = append(props, "URL="+url)
		return NewFileSysClientContext(ctx, dbio.TypeFileS3, props...)
	case strings.HasPrefix(url, "ftp://"):
		props = append(props, "URL="+url)
		return NewFileSysClientContext(ctx, dbio.TypeFileFtp, props...)
	case strings.HasPrefix(url, "sftp://"):
		props = append(props, "URL="+url)
		return NewFileSysClientContext(ctx, dbio.TypeFileSftp, props...)
	case strings.HasPrefix(url, "gs://"):
		props = append(props, "URL="+url)
		return NewFileSysClientContext(ctx, dbio.TypeFileGoogle, props...)
	case strings.Contains(url, ".core.windows.net") || strings.HasPrefix(url, "azure://"):
		return NewFileSysClientContext(ctx, dbio.TypeFileAzure, props...)
	case strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://"):
		props = append(props, "URL="+url)
		return NewFileSysClientContext(ctx, dbio.TypeFileHTTP, props...)
	case strings.HasPrefix(url, "file://"):
		props = append(props, g.F("concurrencyLimit=%d", 20))
		return NewFileSysClientContext(ctx, dbio.TypeFileLocal, props...)
	case strings.Contains(url, "://"):
		err = g.Error("Unable to determine FileSysClient for " + url)
		return
	default:
		props = append(props, g.F("concurrencyLimit=%d", 20))
		return NewFileSysClientContext(ctx, dbio.TypeFileLocal, props...)
	}
}

// PeekFileType peeks into the file to try determine the file type
// CSV is the default
func PeekFileType(reader io.Reader) (ft dbio.FileType, reader2 io.Reader, err error) {

	data, reader2, err := g.Peek(reader, 0)
	if err != nil {
		err = g.Error(err, "could not peek file")
		return
	}

	peekStr := strings.TrimSpace(string(data))
	if strings.HasPrefix(peekStr, "[") || strings.HasPrefix(peekStr, "{") {
		ft = dbio.FileTypeJson
	} else if strings.HasPrefix(peekStr, "<") {
		ft = dbio.FileTypeXml
	} else {
		ft = dbio.FileTypeCsv
	}

	return
}

func makePathSuffix(key string) string {
	if !strings.Contains(key, "*") && !strings.Contains(key, "?") {
		return "*"
	}
	return strings.TrimPrefix(key, GetDeepestParent(key))
}

func NormalizeURI(fs FileSysClient, uri string) string {
	switch fs.FsType() {
	case dbio.TypeFileLocal:
		// to handle windows path style
		uri = strings.ReplaceAll(uri, `\`, `/`)

		return fs.Prefix("") + strings.TrimPrefix(uri, fs.Prefix())
	case dbio.TypeFileSftp:
		path := strings.TrimPrefix(uri, fs.FsType().String()+"://")
		u, err := net.NewURL(uri)
		if strings.Contains(uri, "://") && err == nil {
			path = strings.TrimPrefix(uri, u.U.Scheme+"://")
			path = strings.TrimPrefix(path, u.U.User.Username())
			path = strings.TrimPrefix(path, ":")
			password, _ := u.U.User.Password()
			path = strings.TrimPrefix(path, password)
			path = strings.TrimPrefix(path, "@")
			path = strings.TrimPrefix(path, u.U.Host)
			path = strings.TrimPrefix(path, "/")
		}
		return fs.Prefix("/") + path
	case dbio.TypeFileFtp:
		path := strings.TrimPrefix(uri, fs.FsType().String()+"://")
		u, err := net.NewURL(uri)
		if strings.Contains(uri, "://") && err == nil {
			path = strings.TrimPrefix(uri, u.U.Scheme+"://")
			path = strings.TrimPrefix(path, u.U.User.Username())
			path = strings.TrimPrefix(path, ":")
			password, _ := u.U.User.Password()
			path = strings.TrimPrefix(path, password)
			path = strings.TrimPrefix(path, "@")
			path = strings.TrimPrefix(path, u.U.Host)
			path = strings.TrimPrefix(path, "/")
		}
		return fs.Prefix("/") + path
	default:
		return fs.Prefix("/") + strings.TrimLeft(strings.TrimPrefix(uri, fs.Prefix()), "/")
	}
}

func makeGlob(uri string) (*glob.Glob, error) {
	connType, _, path, err := ParseURLType(uri)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(path, "*") && !strings.Contains(path, "?") {
		return nil, nil
	}

	switch connType {
	case dbio.TypeFileLocal:
		path = strings.TrimPrefix(path, "./")
	case dbio.TypeFileAzure:
		pathContainer := strings.Split(path, "/")[0]
		path = strings.TrimPrefix(path, pathContainer+"/") // remove container
	}

	gc, err := glob.Compile(path)
	if err != nil {
		return nil, err
	}
	return &gc, nil
}

// ParseURL parses a URL
func ParseURL(uri string) (host, path string, err error) {
	_, host, path, err = ParseURLType(uri)
	path = strings.TrimRight(path, makePathSuffix(path))
	return
}

func GetDeepestParent(path string) string {
	parts := strings.Split(path, "/")
	parentParts := []string{}
	for i, part := range parts {
		if strings.Contains(part, "*") || strings.Contains(part, "?") {
			break
		} else if i == len(parts)-1 {
			break
		}
		parentParts = append(parentParts, part)
	}
	if len(parentParts) > 0 && len(parentParts) < len(parts) {
		parentParts = append(parentParts, "") // suffix is "/"
	}
	return strings.Join(parentParts, "/")
}

////////////////////// BASE

// BaseFileSysClient is the base file system type.
type BaseFileSysClient struct {
	FileSysClient
	properties map[string]string
	instance   *FileSysClient
	context    g.Context
	fsType     dbio.Type
	df         *iop.Dataflow
}

// Context provides a pointer to context
func (fs *BaseFileSysClient) Context() (context *g.Context) {
	return &fs.context
}

// Client provides a pointer to itself
func (fs *BaseFileSysClient) Client() *BaseFileSysClient {
	return fs
}

// Close closes the client
func (fs *BaseFileSysClient) Close() error {
	return nil
}

// setDf sets the dataflow
func (fs *BaseFileSysClient) setDf(df *iop.Dataflow) {
	fs.df = df
}

// Instance returns the respective connection Instance
// This is useful to refer back to a subclass method
// from the superclass level. (Aka overloading)
func (fs *BaseFileSysClient) Self() FileSysClient {
	return *fs.instance
}

// FsType return the type of the client
func (fs *BaseFileSysClient) FsType() dbio.Type {
	return fs.fsType
}

// Buckets returns the buckets found in the account
func (fs *BaseFileSysClient) Buckets() (paths []string, err error) {
	return
}

// Prefix returns the url prefix
func (fs *BaseFileSysClient) Prefix(suffix ...string) string {
	return fs.Self().Prefix(suffix...)
}

// Query queries the file system via duckdb
func (fs *BaseFileSysClient) Query(uri, sql string) (data iop.Dataset, err error) {
	props := g.MapToKVArr(map[string]string{"fs_props": g.Marshal(fs.Props())})
	duck := iop.NewDuckDb(fs.context.Ctx, props...)
	duck.AddExtension("iceberg")
	duck.AddExtension("delta")

	_ = duck.PrepareFsSecretAndURI(uri)

	data, err = duck.Query(sql)
	if err != nil {
		return data, g.Error(err, "could not query duckdb")
	}

	return
}

// GetProp returns the value of a property
func (fs *BaseFileSysClient) GetProp(key string, keys ...string) string {
	fs.context.Mux.Lock()
	val := fs.properties[strings.ToLower(key)]
	for _, key := range keys {
		if val != "" {
			break
		}
		val = fs.properties[strings.ToLower(key)]
	}
	fs.context.Mux.Unlock()
	return val
}

// SetProp sets the value of a property
func (fs *BaseFileSysClient) SetProp(key string, val string) {
	fs.context.Mux.Lock()
	if fs.properties == nil {
		fs.properties = map[string]string{}
	}
	fs.properties[strings.ToLower(key)] = val
	fs.context.Mux.Unlock()
}

// Props returns a copy of the properties map
func (fs *BaseFileSysClient) Props() map[string]string {
	m := map[string]string{}
	fs.context.Mux.Lock()
	for k, v := range fs.properties {
		m[k] = v
	}
	fs.context.Mux.Unlock()
	return m
}

func (fs *BaseFileSysClient) GetRefTs() time.Time {
	var ts time.Time
	if val := fs.GetProp("SLING_FS_TIMESTAMP"); val != "" {
		if valInt := cast.ToInt64(val); valInt > 0 {
			ts = time.Unix(valInt, 0)
		} else {
			ts = cast.ToTime(val)
		}
		if gte := os.Getenv("SLING_GREATER_THAN_EQUAL"); gte != "" && cast.ToBool(gte) && !ts.IsZero() {
			ts = ts.Add(-1 * time.Millisecond)
		}
	}
	return ts
}

// GetDatastream return a datastream for the given path
func (fs *BaseFileSysClient) GetDatastream(uri string, cfg ...iop.FileStreamConfig) (ds *iop.Datastream, err error) {
	Cfg := iop.FileStreamConfig{} // infinite
	if len(cfg) > 0 {
		Cfg = cfg[0]
	}

	ds = iop.NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetMetadata(fs.GetProp("METADATA"))
	ds.Metadata.StreamURL.Value = uri
	ds.SetConfig(fs.Props())

	if Cfg.Format == dbio.FileTypeNone {
		Cfg.Format = InferFileFormat(uri)
	}

	go func() {
		// recover from panic
		defer func() {
			if r := recover(); r != nil {
				err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
				ds.Context.CaptureErr(err)
			}
		}()

		// manage concurrency
		defer fs.Context().Wg.Read.Done()
		fs.Context().Wg.Read.Add()

		g.Debug("reading datastream from %s [format=%s]", uri, Cfg.Format)

		// no reader needed for iceberg, delta, duckdb will handle it
		if Cfg.ShouldUseDuckDB() {
			Cfg.Props = map[string]string{"fs_props": g.Marshal(fs.Props())}
			switch Cfg.Format {
			case dbio.FileTypeIceberg:
				err = ds.ConsumeIcebergReader(uri, Cfg)
			case dbio.FileTypeDelta:
				err = ds.ConsumeDeltaReader(uri, Cfg)
			case dbio.FileTypeParquet:
				err = ds.ConsumeParquetReaderDuckDb(uri, Cfg)
			case dbio.FileTypeCsv:
				err = ds.ConsumeCsvReaderDuckDb(uri, Cfg)
			}

			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "Error consuming reader for %s", uri))
			}
			return
		}

		reader, err := fs.Self().GetReader(uri)
		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "error getting reader"))
			return
		}

		// Wait for reader to start reading or err
		for {
			// Try peeking
			if b := bufio.NewReader(reader).Size(); b > 0 {
				break
			}

			if fs.Context().Err() != nil {
				// has errorred
				return
			}
			time.Sleep(50 * time.Millisecond)
		}

		switch Cfg.Format {
		case dbio.FileTypeJson:
			err = ds.ConsumeJsonReader(reader)
		case dbio.FileTypeXml:
			err = ds.ConsumeXmlReader(reader)
		case dbio.FileTypeParquet:
			err = ds.ConsumeParquetReader(reader)
		case dbio.FileTypeAvro:
			err = ds.ConsumeAvroReader(reader)
		case dbio.FileTypeSAS:
			err = ds.ConsumeSASReader(reader)
		case dbio.FileTypeExcel:
			err = ds.ConsumeExcelReader(reader, fs.properties)
		case dbio.FileTypeCsv:
			err = ds.ConsumeCsvReader(reader)
		default:
			g.Warn("GetDatastream | File Format not recognized: %s. Using CSV parsing", Cfg.Format)
			err = ds.ConsumeCsvReader(reader)
		}

		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Error consuming reader for %s", uri))
		}

	}()

	return ds, err
}

// ReadDataflow read
func (fs *BaseFileSysClient) ReadDataflow(url string, cfg ...iop.FileStreamConfig) (df *iop.Dataflow, err error) {
	Cfg := iop.FileStreamConfig{} // infinite
	if len(cfg) > 0 {
		Cfg = cfg[0]
	}

	if Cfg.Format == dbio.FileTypeNone {
		Cfg.Format = dbio.FileType(strings.ToLower(cast.ToString(fs.GetProp("FORMAT"))))
		if Cfg.Format == dbio.FileTypeNone {
			Cfg.Format = InferFileFormat(url, dbio.FileTypeNone)
		}
	}

	if strings.HasSuffix(strings.ToLower(url), ".zip") {
		localFs, err := NewFileSysClient(dbio.TypeFileLocal)
		if err != nil {
			return df, g.Error(err, "could not initialize localFs")
		}

		reader, err := fs.Self().GetReader(url)
		if err != nil {
			return df, g.Error(err, "could not get zip reader")
		}

		folderPath := path.Join(env.GetTempFolder(), "dbio_temp_")

		zipPath := folderPath + ".zip"
		_, err = localFs.Write(zipPath, reader)
		if err != nil {
			return df, g.Error(err, "could not write to "+zipPath)
		}

		nodeMaps, err := iop.Unzip(zipPath, folderPath)
		if err != nil {
			return df, g.Error(err, "Error unzipping")
		}
		// delete zip file
		Delete(localFs, zipPath)

		nodes := NewFileNodes(nodeMaps)
		df, err = GetDataflow(localFs.Self(), nodes, Cfg)
		if err != nil {
			return df, g.Error(err, "Error making dataflow")
		}

		// delete unzipped folder when done
		df.Defer(func() { Delete(localFs, folderPath) })

		return df, nil
	}

	var nodes FileNodes
	if Cfg.ShouldUseDuckDB() {
		nodes = FileNodes{FileNode{URI: url}}
	} else {
		g.Trace("listing path: %s", url)
		nodes, err = fs.Self().ListRecursive(url)
		if err != nil {
			err = g.Error(err, "Error getting paths")
			return
		}

		nodes.Sort()
	}

	df, err = GetDataflow(fs.Self(), nodes, Cfg)
	if err != nil {
		err = g.Error(err, "error getting dataflow")
		return
	}

	df.FsURL = url
	return
}

// WriteDataflow writes a dataflow to a file sys.
func WriteDataflow(fs FileSysClient, df *iop.Dataflow, url string) (bw int64, err error) {

	// if ignore_existing is specified, check if files exists.
	// if exists, then don't delete / overwrite
	if cast.ToBool(fs.GetProp("ignore_existing")) {
		paths, err := fs.List(url)
		if err != nil {
			if g.IsDebugLow() {
				g.Warn("could not list path %s\n%s", url, err.Error())
			}
			err = nil
		}

		if len(paths) > 0 {
			g.Debug("not writing since file/folder exists at %s (ignore_existing=true)", url)

			// close datastreams
			for _, ds := range df.Streams {
				ds.Close()
			}
			df.Close() // close dataflow
			return 0, nil
		}
	}

	fileReadyChn := make(chan FileReady, 10000)

	g.Trace("writing dataflow to %s", url)
	go func() {
		for range fileReadyChn {
			// do nothing, wait for completion
		}
	}()

	return fs.Self().WriteDataflowReady(df, url, fileReadyChn, nil)
}

// GetReaders returns one or more readers from specified paths in specified FileSysClient
func (fs *BaseFileSysClient) GetReaders(paths ...string) (readers []io.Reader, err error) {
	if len(paths) == 0 {
		err = g.Error("Provided 0 files for: %#v", paths)
		return
	}

	for _, path := range paths {
		reader, err := fs.Self().GetReader(path)
		if err != nil {
			return nil, g.Error(err, "Unable to process "+path)
		}
		readers = append(readers, reader)
	}

	return readers, nil
}

type FileReady struct {
	Columns iop.Columns
	Node    FileNode
	BytesW  int64
	BatchID string
}

// WriteDataflowReady writes to a file sys and notifies the fileReady chan.
func (fs *BaseFileSysClient) WriteDataflowReady(df *iop.Dataflow, url string, fileReadyChn chan FileReady, sc *iop.StreamConfig) (bw int64, err error) {
	fsClient := fs.Self()
	defer close(fileReadyChn)
	useBufferedStream := cast.ToBool(fs.GetProp("USE_BUFFERED_STREAM"))
	concurrency := cast.ToInt(fs.GetProp("CONCURRENCY"))
	compression := iop.CompressorType(strings.ToLower(fs.GetProp("COMPRESSION")))
	fileFormat := dbio.FileType(strings.ToLower(cast.ToString(fs.GetProp("FORMAT"))))
	fileRowLimit := cast.ToInt(fs.GetProp("FILE_MAX_ROWS"))
	fileBytesLimit := cast.ToInt64(fs.GetProp("FILE_MAX_BYTES")) // uncompressed file size
	fileExt := cast.ToString(fs.GetProp("FILE_EXTENSION"))

	// set default concurrency
	// let's set 7 as a safe limit
	if concurrency == 0 {
		concurrency = 7
	}

	// concurrency should not be higher than number of CPUs
	if concurrency > runtime.NumCPU() {
		concurrency = runtime.NumCPU()
	}

	if fileFormat == dbio.FileTypeNone {
		fileFormat = InferFileFormat(url)
	}

	url = strings.TrimSuffix(NormalizeURI(fs, url), "/")

	singleFile := fileRowLimit == 0 && fileBytesLimit == 0

	// parse file partitioning notation (*), determine single-file vs folder mode
	parts := strings.Split(url, "/")
	if lastPart := parts[len(parts)-1]; strings.HasPrefix(lastPart, "*") {
		singleFile = false
		// set partition file defaults
		fileRowLimit = lo.Ternary(fileRowLimit == 0, 100000, fileRowLimit)
		fileBytesLimit = lo.Ternary(fileBytesLimit == 0, 50000000, fileBytesLimit)
		if suffix := strings.TrimPrefix(lastPart, "*"); suffix != "" {
			fileExt = suffix
		}
		url = strings.TrimSuffix(url, "/"+lastPart)
	}

	// adjust fileBytesLimit due to compression
	if g.In(compression, iop.GzipCompressorType, iop.ZStandardCompressorType, iop.SnappyCompressorType) {
		fileBytesLimit = fileBytesLimit * 6 // compressed, multiply
	}

	if sc != nil {
		df.SetConfig(sc)
	}

	processStream := func(ds *iop.Datastream, partURL string) {
		defer df.Context.Wg.Read.Done()
		localCtx := g.NewContext(ds.Context.Ctx, concurrency)

		writePart := func(reader io.Reader, batchR *iop.BatchReader, partURL string) {
			defer localCtx.Wg.Read.Done()

			bw0, err := fsClient.Write(partURL, reader)
			bID := lo.Ternary(batchR.Batch != nil, batchR.Batch.ID(), "")
			node := FileNode{URI: partURL, Size: cast.ToUint64(bw0)}
			fileReadyChn <- FileReady{batchR.Columns, node, bw0, bID}

			if err != nil {
				g.LogError(err)
				df.Context.CaptureErr(g.Error(err))
				ds.Context.CaptureErr(g.Error(err))
				io.Copy(io.Discard, reader) // flush it out so it can close
			}
			g.Trace("wrote %s [%d rows] to %s", humanize.Bytes(cast.ToUint64(bw0)), batchR.Counter, partURL)
			bw += bw0
			df.AddEgressBytes(uint64(bw0))
		}

		// pre-add to WG to not hold next reader in memory while waiting
		localCtx.Wg.Read.Add()
		fileCount := 0

		processReader := func(batchR *iop.BatchReader) error {
			fileCount++
			fileSuffix := lo.Ternary(fileExt == "", fileFormat.Ext(), fileExt)
			subPartURL := fmt.Sprintf("%s.%04d%s", partURL, fileCount, fileSuffix)
			if singleFile {
				subPartURL = partURL
				for _, comp := range []iop.CompressorType{
					iop.GzipCompressorType,
					iop.SnappyCompressorType,
					iop.ZStandardCompressorType,
				} {
					compressor := iop.NewCompressor(comp)
					if strings.HasSuffix(subPartURL, compressor.Suffix()) {
						compression = comp
						subPartURL = strings.TrimSuffix(subPartURL, compressor.Suffix())
						break
					}
				}
			}

			compressor := iop.NewCompressor(compression)
			if fileFormat == dbio.FileTypeParquet {
				compressor = iop.NewCompressor("none") // compression is done internally
			} else {
				subPartURL = subPartURL + compressor.Suffix()
			}

			g.Trace("writing stream to " + subPartURL)
			go writePart(compressor.Compress(batchR.Reader), batchR, subPartURL)
			localCtx.Wg.Read.Add()
			// localCtx.MemBasedLimit(98) // wait until memory is lower than 90%

			return df.Err()
		}

		switch fileFormat {
		case dbio.FileTypeJson:
			for reader := range ds.NewJsonReaderChnl(fileRowLimit, fileBytesLimit) {
				err := processReader(&iop.BatchReader{Columns: ds.Columns, Reader: reader, Counter: -1, Batch: ds.CurrentBatch})
				if err != nil {
					break
				}
			}
		case dbio.FileTypeJsonLines:
			for reader := range ds.NewJsonLinesReaderChnl(fileRowLimit, fileBytesLimit) {
				err := processReader(&iop.BatchReader{Columns: ds.Columns, Reader: reader, Counter: -1, Batch: ds.CurrentBatch})
				if err != nil {
					break
				}
			}
		case dbio.FileTypeParquet:
			for reader := range ds.NewParquetReaderChnl(fileRowLimit, fileBytesLimit, compression) {
				err := processReader(reader)
				if err != nil {
					break
				}
			}
		case dbio.FileTypeExcel:
			for reader := range ds.NewExcelReaderChnl(fileRowLimit, fileBytesLimit, fs.GetProp("sheet")) {
				err := processReader(reader)
				if err != nil {
					break
				}
			}
		case dbio.FileTypeCsv:
			if useBufferedStream {
				// faster, but dangerous. Holds data in memory
				for reader := range ds.NewCsvBufferReaderChnl(fileRowLimit, fileBytesLimit) {
					err := processReader(&iop.BatchReader{Columns: ds.Columns, Reader: reader, Counter: -1, Batch: ds.CurrentBatch})
					if err != nil {
						break
					}
				}
			} else {
				// slower! but safer, waits for compression but does not hold data in memory
				for batchR := range ds.NewCsvReaderChnl(fileRowLimit, fileBytesLimit) {
					err := processReader(batchR)
					if err != nil {
						break
					}
				}
			}
		default:
			g.Warn("WriteDataflowReady | File Format not recognized: %s", fileFormat)
		}

		ds.Buffer = nil // clear buffer
		if ds.Err() != nil {
			df.Context.CaptureErr(g.Error(ds.Err()))
		}
		localCtx.Wg.Read.Done() // clear that pre-added WG
		localCtx.Wg.Read.Wait()
	}

	err = Delete(fsClient, url)
	if err != nil {
		err = g.Error(err, "Could not delete url")
		return
	}

	if !singleFile && g.In(fsClient.FsType(), dbio.TypeFileLocal, dbio.TypeFileSftp, dbio.TypeFileFtp) {
		path, err := fsClient.GetPath(url)
		if err != nil {
			return 0, g.Error(err, "Error Parsing url: "+url)
		}

		err = fsClient.MkdirAll(path)
		if err != nil {
			return 0, g.Error(err, "could not create directory")
		}
	}

	partCnt := 1

	var streamCh chan *iop.Datastream
	if singleFile {
		// merge dataflow streams into one stream
		streamCh = make(chan *iop.Datastream)
		go func() {
			streamCh <- iop.MergeDataflow(df)
			close(streamCh)
		}()
	} else {
		streamCh = df.StreamCh
	}

	// for ds := range df.MakeStreamCh(true) {
	for ds := range streamCh {

		partURL := fmt.Sprintf("%s/part.%02d", url, partCnt)
		if singleFile {
			partURL = url
		}

		g.DebugLow("writing to %s [fileRowLimit=%d fileBytesLimit=%d compression=%s concurrency=%d useBufferedStream=%v fileFormat=%v singleFile=%v]", partURL, fileRowLimit, fileBytesLimit, compression, concurrency, useBufferedStream, fileFormat, singleFile)

		df.Context.Wg.Read.Add()
		ds.SetConfig(fs.Props()) // pass options
		go processStream(ds, partURL)
		partCnt++
	}

	df.Context.Wg.Read.Wait()
	if df.Err() != nil {
		err = g.Error(df.Err())
	}

	return
}

// Delete deletes the provided path before writing
// with some safeguards so to not accidentally delete some root path
func Delete(fs FileSysClient, uri string) (err error) {
	uri = NormalizeURI(fs, uri)

	host, path, err := ParseURL(uri)
	if err != nil {
		return g.Error(err, "could not parse %s", uri)
	}

	// add some safeguards
	p := strings.TrimPrefix(strings.TrimSuffix(path, "/"), "/")
	pArr := strings.Split(p, "/")

	switch fs.FsType() {
	case dbio.TypeFileS3, dbio.TypeFileGoogle:
		if len(p) == 0 {
			return g.Error("invalid uri / path for overwriting (bucket): %s", uri)
		}
	case dbio.TypeFileAzure:
		if len(p) == 0 {
			return g.Error("invalid uri / path for overwriting (account): %s", uri)
		}
		// container level
		if len(pArr) <= 1 {
			return g.Error("invalid uri / path for overwriting (container): %s", uri)
		}
	case dbio.TypeFileLocal:
		if len(host) == 0 && len(p) == 0 {
			return g.Error("invalid uri / path for overwriting (root): %s", uri)
		}
	case dbio.TypeFileSftp:
		if len(p) == 0 {
			return g.Error("invalid uri / path for overwriting (root): %s", uri)
		}
	case dbio.TypeFileFtp:
		if len(p) == 0 {
			return g.Error("invalid uri / path for overwriting (root): %s", uri)
		}
	}

	err = fs.delete(uri)
	if err != nil && !strings.Contains(err.Error(), "exist") {
		if g.IsDebugLow() {
			g.Warn("could not delete path %s\n%s", uri, err.Error())
		}
		err = nil
	}

	return nil
}

// GetDataflow returns a dataflow from specified paths in specified FileSysClient
func GetDataflow(fs FileSysClient, nodes FileNodes, cfg iop.FileStreamConfig) (df *iop.Dataflow, err error) {
	if cfg.Format == dbio.FileTypeNone {
		cfg.Format = dbio.FileType(strings.ToLower(cast.ToString(fs.GetProp("FORMAT"))))
	}

	if len(nodes) == 0 {
		err = g.Error("Provided 0 files for: %#v", nodes)
		return
	}

	df = iop.NewDataflowContext(fs.Context().Ctx, cfg.Limit)
	dsCh := make(chan *iop.Datastream)
	fs.setDf(df)

	go func() {
		defer close(dsCh)

		allowMerging := strings.ToLower(os.Getenv("SLING_MERGE_READERS")) != "false" && !cfg.ShouldUseDuckDB()

		pushDatastream := func(ds *iop.Datastream) {
			// use selected fields only when not parquet
			skipSelect := g.In(cfg.Format, dbio.FileTypeParquet, dbio.FileTypeIceberg, dbio.FileTypeDelta) || cfg.ShouldUseDuckDB()
			if len(cfg.Select) > 1 && !skipSelect {
				cols := iop.NewColumnsFromFields(cfg.Select...)
				fm := ds.Columns.FieldMap(true)
				ds.Columns.DbTypes()
				transf := func(in []interface{}) (out []interface{}) {
					for _, col := range cols {
						if i, ok := fm[strings.ToLower(col.Name)]; ok {
							out = append(out, in[i])
						} else {
							df.Context.CaptureErr(g.Error("column %s not found", col.Name))
						}
					}
					return
				}
				dsCh <- ds.Map(cols, transf)
			} else {
				dsCh <- ds
			}
		}

		if allowMerging && (cfg.Format.IsJson() || isFiletype(dbio.FileTypeJson, nodes.URIs()...) || isFiletype(dbio.FileTypeJsonLines, nodes.URIs()...)) {
			ds, err := MergeReaders(fs, dbio.FileTypeJson, nodes, cfg.Limit)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "Unable to merge paths at %s", fs.GetProp("url")))
				return
			}

			pushDatastream(ds)
			return // done
		}

		if allowMerging && (cfg.Format == dbio.FileTypeXml || isFiletype(dbio.FileTypeXml, nodes.URIs()...)) {
			ds, err := MergeReaders(fs, dbio.FileTypeXml, nodes, cfg.Limit)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "Unable to merge paths at %s", fs.GetProp("url")))
				return
			}

			pushDatastream(ds)
			return // done
		}

		// csvs
		if allowMerging && (cfg.Format == dbio.FileTypeCsv || isFiletype(dbio.FileTypeCsv, nodes.URIs()...)) {
			ds, err := MergeReaders(fs, dbio.FileTypeCsv, nodes, cfg.Limit)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "Unable to merge paths at %s", fs.GetProp("url")))
				return
			}
			pushDatastream(ds)
			return // done
		}

		for _, node := range nodes {
			uri := node.URI
			if strings.HasSuffix(uri, "/") {
				// allow iceberg/delta tables to be read as directories
				if g.In(cfg.Format, dbio.FileTypeIceberg, dbio.FileTypeDelta) {
					uri = strings.TrimSuffix(uri, "/") // remove trailing slash
				} else {
					g.DebugLow("skipping %s because is not file", uri)
					continue
				}
			}

			ds, err := fs.GetDatastream(uri, cfg)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "Unable to process "+uri))
				return
			}
			pushDatastream(ds)

			// when pulling from local disk, process one file at a time
			if fs.FsType() == dbio.TypeFileLocal {
				ds.WaitClosed()
			}
		}

	}()

	go df.PushStreamChan(dsCh)

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, g.Error(err)
	}

	return df, nil
}

// MakeDatastream create a datastream from a reader
func MakeDatastream(reader io.Reader, cfg map[string]string) (ds *iop.Datastream, err error) {

	data, reader2, err := g.Peek(reader, 0)
	if err != nil {
		return nil, err
	}

	peekStr := string(data)
	if strings.HasPrefix(peekStr, "[") || strings.HasPrefix(peekStr, "{") {
		ds = iop.NewDatastream(iop.Columns{})
		ds.SafeInference = true
		ds.SetConfig(cfg)
		err = ds.ConsumeJsonReader(reader2)
		if err != nil {
			return nil, err
		}
	} else {
		csv := iop.CSV{Reader: reader2, Config: cfg}
		ds, err = csv.ReadStream()
		if err != nil {
			return nil, err
		}
	}

	return ds, nil
}

// WriteDatastream writes a datasream to a writer
// or use fs.Write(path, ds.NewCsvReader(0))
func WriteDatastream(writer io.Writer, ds *iop.Datastream) (bw int64, err error) {
	reader := ds.NewCsvReader(0, 0)
	return Write(reader, writer)
}

// Write writer to a writer from a reader
func Write(reader io.Reader, writer io.Writer) (bw int64, err error) {
	bw, err = io.Copy(writer, reader)
	if err != nil {
		err = g.Error(err, "Error writing from reader")
	}
	return
}

// TestFsPermissions tests read/write permisions
func TestFsPermissions(fs FileSysClient, pathURL string) (err error) {
	testString := "abcde"

	// Create file/folder
	bw, err := fs.Write(pathURL, strings.NewReader(testString))
	if err != nil {
		return g.Error(err, "failed testing permissions: Create file/folder")
	} else if bw == 0 {
		return g.Error("failed testing permissions: Create file/folder returned 0 bytes")
	}

	// List File
	paths, err := fs.List(pathURL)
	if err != nil {
		return g.Error(err, "failed testing permissions: List File")
	} else if len(paths) == 0 {
		return g.Error("failed testing permissions: List File is zero")
	}

	// Read File
	reader, err := fs.GetReader(pathURL)
	if err != nil {
		return g.Error(err, "failed testing permissions: Read File")
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		return g.Error(err, "failed testing permissions: Read File, reading reader")
	}

	if string(content) != testString {
		return g.Error("failed testing permissions: Read File content mismatch")
	}

	// Delete file/folder
	err = Delete(fs, pathURL)
	if err != nil {
		return g.Error(err, "failed testing permissions: Delete file/folder")
	}

	return
}

func isFiletype(fileType dbio.FileType, paths ...string) bool {
	fileCnt := 0
	dirCnt := 0

	ext := fileType.Ext()
	for _, path := range paths {
		if strings.HasSuffix(path, "/") {
			dirCnt++
			continue
		}

		if strings.HasSuffix(path, ext) || strings.Contains(path, ext+".") {
			fileCnt++
		}
	}
	return fileCnt > 0 && len(paths) == fileCnt+dirCnt
}

func MergeReaders(fs FileSysClient, fileType dbio.FileType, nodes FileNodes, limit int) (ds *iop.Datastream, err error) {
	if len(nodes) == 0 {
		err = g.Error("Provided 0 files for: %#v", nodes)
		return
	}

	// infer if missing
	if string(fileType) == "" {
		fileType = InferFileFormat(nodes[0].URI)
	}

	pipeR, pipeW := io.Pipe()

	url := fs.GetProp("url")
	ds = iop.NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetMetadata(fs.GetProp("METADATA"))
	ds.Metadata.StreamURL.Value = url
	ds.SetConfig(fs.Client().Props())
	g.Debug("reading single datastream from %s [format=%s]", url, fileType)

	setError := func(err error) {
		ds.Context.CaptureErr(err)
		ds.Context.Cancel()
		fs.Context().CaptureErr(err)
		fs.Context().Cancel()
		g.LogError(err)
	}

	g.DebugLow("merging %s readers of %d files (sequential processing) from %s", fileType, len(nodes), url)
	readerChn := make(chan *iop.ReaderReady)
	go func() {
		defer close(readerChn)

		for _, path := range nodes.URIs() {
			if strings.HasSuffix(path, "/") {
				g.DebugLow("skipping %s because is not file", path)
				continue
			}

			g.Debug("processing reader from %s", path)

			reader, err := fs.Self().GetReader(path)
			if err != nil {
				setError(g.Error(err, "Error getting reader"))
				return
			}

			r := &iop.ReaderReady{Reader: reader, URI: path}
			readerChn <- r
		}

	}()

	if g.In(fileType, dbio.FileTypeCsv, dbio.FileTypeJson, dbio.FileTypeJsonLines, dbio.FileTypeXml) {
		pipeW.Close()

		switch fileType {
		case dbio.FileTypeJson, dbio.FileTypeJsonLines:
			err = ds.ConsumeJsonReaderChl(readerChn, false)
		case dbio.FileTypeXml:
			err = ds.ConsumeJsonReaderChl(readerChn, true)
		case dbio.FileTypeCsv:
			err = ds.ConsumeCsvReaderChl(readerChn)
		}

	} else {

		go func() {
			defer pipeW.Close()

			for reader := range readerChn {
				_, err = io.Copy(pipeW, reader.Reader)
				if err != nil {
					setError(g.Error(err, "Error copying reader to pipe writer"))
					return
				}

				if limit > 0 && (ds.Limited(limit) || len(ds.Buffer) >= limit) {
					return
				}
			}
		}()

		switch fileType {
		case dbio.FileTypeJson, dbio.FileTypeJsonLines:
			err = ds.ConsumeJsonReader(pipeR)
		case dbio.FileTypeXml:
			err = ds.ConsumeXmlReader(pipeR)
		case dbio.FileTypeCsv:
			err = ds.ConsumeCsvReader(pipeR)
		default:
			return ds, g.Error("unrecognized fileType (%s) for MergeReaders", fileType)
		}
	}
	if err != nil {
		return ds, g.Error(err, "Error consuming reader for fileType: '%s'", fileType)
	}

	return ds, nil
}

func ProcessStreamViaTempFile(ds *iop.Datastream) (nDs *iop.Datastream, err error) {
	// temp file
	tempDir := env.GetTempFolder()
	filePath := path.Join(tempDir, g.NewTsID("sling.temp")+".csv")

	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	if err != nil {
		return nil, g.Error(err, "could not obtain client for temp file in ProcessStreamViaTempFile")
	}

	err = ds.WaitReady()
	if err != nil {
		return nil, err
	}

	g.Debug("writing to temp file %s", filePath)
	_, err = fs.Write("file://"+filePath, ds.NewCsvReader(0, 0))
	if err != nil {
		return nil, g.Error(err, "could not write to temp file for ProcessStreamViaTempFile")
	}

	nDs = iop.NewDatastreamContext(ds.Context.Ctx, ds.Columns)
	nDs.Inferred = true
	config := ds.GetConfig()
	config["fields_per_rec"] = "-1" // allow different number of records per line
	nDs.SetConfig(config)
	nDs.Defer(func() { env.RemoveLocalTempFile(filePath) })

	file, err := os.Open(filePath)
	if err != nil {
		err = g.Error(err, "Unable to open temp file: "+filePath)
		return nil, err
	}

	err = nDs.ConsumeCsvReader(file)
	if err != nil {
		os.Remove(filePath)
		return nDs, g.Error(err, "could not consume temp file for ProcessStreamViaTempFile")
	}

	return nDs, nil
}

func InferFileFormat(path string, defaults ...dbio.FileType) dbio.FileType {
	path = strings.TrimSpace(strings.ToLower(path))

	for _, fileType := range []dbio.FileType{dbio.FileTypeCsv, dbio.FileTypeJsonLines, dbio.FileTypeJson, dbio.FileTypeXml, dbio.FileTypeParquet, dbio.FileTypeAvro, dbio.FileTypeSAS, dbio.FileTypeExcel} {
		ext := fileType.Ext()
		if strings.HasSuffix(path, ext) || strings.Contains(path, ext+".") {
			return fileType
		}
	}

	if len(defaults) > 0 {
		return defaults[0]
	}

	// default is csv
	return dbio.FileTypeCsv
}
