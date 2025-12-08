package sling

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestGetRate(t *testing.T) {
	now := time.Now()
	now2 := time.Now()
	df := iop.Dataflow{}
	task := TaskExecution{
		StartTime: &now,
		EndTime:   &now2,
		//df:        &df,
	}
	rate, _ := task.GetRate(10)

	st := *task.StartTime
	et := *task.EndTime

	g.P(et.UnixNano())
	g.P(st.UnixNano())
	g.P(df.Count())
	g.P(rate)

	g.P(et.UnixNano() - st.UnixNano())

	secElapsed := cast.ToFloat64(et.UnixNano()-st.UnixNano()) / 1000000000.0
	g.P(secElapsed)
	g.P(math.Round(cast.ToFloat64(df.Count()) / secElapsed))
	rate = cast.ToInt64(math.Round(cast.ToFloat64(df.Count()) / secElapsed))
	g.P(rate)
}

func TestConfig(t *testing.T) {

	cfgStr := `{
		"post_dbt": {
			"conn": "ORACLE_SLING",
			"expr": "my_first_dbt_model",
			"name": "DBT_PROJ_1",
			"folder": "/",
			"version": "0.18.0",
			"repo_url": "https://github.com/fishtown-analytics/dbt-starter-project"
		},
		"tgt_conn": "ORACLE_SLING"
	}`
	_, err := NewConfig(cfgStr)
	assert.NoError(t, err)

}

func TestSkipIncrementalCheckpointFlag(t *testing.T) {
	t.Run("config flag only", func(t *testing.T) {
		cfgStr := `
mode: incremental
source:
  conn: LOCAL
  stream: /tmp/file.csv
  options:
    skip_incremental_checkpoint: true
target:
  conn: PROTON_DB
  object: db.table
`
		cfg, err := NewConfig(cfgStr)
		assert.NoError(t, err)
		assert.NotNil(t, cfg.Source.Options)
		assert.NotNil(t, cfg.Source.Options.SkipIncrementalCheckpoint)
		assert.True(t, *cfg.Source.Options.SkipIncrementalCheckpoint)
		assert.True(t, cfg.SkipIncrementalCheckpoint())
	})

	t.Run("env var only", func(t *testing.T) {
		orig := os.Getenv("SLING_SKIP_INCREMENTAL_CHECKPOINT")
		defer os.Setenv("SLING_SKIP_INCREMENTAL_CHECKPOINT", orig)

		_ = os.Setenv("SLING_SKIP_INCREMENTAL_CHECKPOINT", "TRUE")

		cfgStr := `
mode: incremental
source:
  conn: LOCAL
  stream: /tmp/file.csv
target:
  conn: PROTON_DB
  object: db.table
`
		cfg, err := NewConfig(cfgStr)
		assert.NoError(t, err)
		// no explicit flag in config
		if cfg.Source.Options != nil {
			assert.Nil(t, cfg.Source.Options.SkipIncrementalCheckpoint)
		}
		assert.True(t, cfg.SkipIncrementalCheckpoint())
	})
}

func TestFileUpdateKeyShorthandDot(t *testing.T) {
	t.Run("normalize dot to loaded_at", func(t *testing.T) {
		cfgStr := `
mode: incremental
source:
  conn: LOCAL
  stream: /tmp/file.csv
  update_key: "."
target:
  conn: PROTON_DB
  object: db.table
`
		cfg, err := NewConfig(cfgStr)
		assert.NoError(t, err)
		assert.Equal(t, slingLoadedAtColumn, cfg.Source.UpdateKey)
		if assert.NotNil(t, cfg.MetadataLoadedAt) {
			assert.True(t, *cfg.MetadataLoadedAt)
		}
	})

	t.Run("dot shorthand not allowed for db source", func(t *testing.T) {
		cfgStr := `
mode: incremental
source:
  conn: POSTGRES
  stream: public.tbl
  update_key: "."
target:
  conn: PROTON_DB
  object: db.table
`
		_, err := NewConfig(cfgStr)
		assert.Error(t, err)
	})

	t.Run("normalize when skipping checkpoint", func(t *testing.T) {
		cfgStr := `
mode: incremental
source:
  conn: LOCAL
  stream: /tmp/file.csv
  update_key: "."
  options:
    skip_incremental_checkpoint: true
target:
  conn: PROTON_DB
  object: db.table
`
		cfg, err := NewConfig(cfgStr)
		assert.NoError(t, err)
		assert.Equal(t, slingLoadedAtColumn, cfg.Source.UpdateKey)
		if assert.NotNil(t, cfg.MetadataLoadedAt) {
			assert.True(t, *cfg.MetadataLoadedAt)
		}
		assert.True(t, cfg.SkipIncrementalCheckpoint())
	})
}

func TestColumnCasing(t *testing.T) {
	df := iop.NewDataflow(0)

	sourceCasing := SourceColumnCasing
	snakeCasing := SnakeColumnCasing
	targetCasing := TargetColumnCasing

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"})
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &sourceCasing)
	assert.Equal(t, "myCol", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"}, iop.Column{Name: "hey-hey"})
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &snakeCasing)
	assert.Equal(t, "MY_COL", df.Columns[0].Name)
	assert.Equal(t, "HEY_HEY", df.Columns[1].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"})
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &targetCasing)
	assert.Equal(t, "MYCOL", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "DHL OriginalTracking-Number"})
	applyColumnCasingToDf(df, dbio.TypeDbDuckDb, &targetCasing)
	assert.Equal(t, "dhl_originaltracking_number", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "DHL OriginalTracking-Number"})
	applyColumnCasingToDf(df, dbio.TypeDbDuckDb, &snakeCasing)
	assert.Equal(t, "dhl_original_tracking_number", df.Columns[0].Name)
}
