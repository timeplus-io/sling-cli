package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type CleanPartitionsConfig struct {
	TimeplusdConn string   `yaml:"timeplusd_conn"`
	Tables        []string `yaml:"tables"`
}

func processCleanPartitions(c *g.CliSC) (bool, error) {
	configPath := cast.ToString(c.Vals["config"])
	if configPath == "" {
		return false, g.Error("Configuration file path is required")
	}

	cfg, err := readCleanPartitionsConfig(configPath)
	if err != nil {
		return false, g.Error(err, "failure reading clean partitions configuration")
	}

	err = runCleanPartitions(cfg)
	if err != nil {
		return false, g.Error(err, "failure running clean partitions")
	}

	return true, nil
}

func readCleanPartitionsConfig(configPath string) (*CleanPartitionsConfig, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, g.Error(err, "Error reading config file")
	}

	var config CleanPartitionsConfig
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		return nil, g.Error(err, "Error parsing config file")
	}

	return &config, nil
}

func runCleanPartitions(cfg *CleanPartitionsConfig) error {
	conn, err := database.NewConn(cfg.TimeplusdConn)
	if err != nil {
		return g.Error(err, "Error connecting to timeplus database")
	}
	defer conn.Close()

	today := time.Now().Format("20060102")

	for _, table := range cfg.Tables {
		err := cleanTablePartitions(conn, table, today)
		if err != nil {
			g.Warn("Error cleaning partitions for table %s: %v", table, err)
		}
	}

	return nil
}

func cleanTablePartitions(conn database.Connection, table, date string) error {
	partitions, err := getPartitions(conn, table, date)
	if err != nil {
		return g.Error(err, "Error getting partitions for table: %s", table)
	}
	if len(partitions) > 0 {
		g.Info("Retrieved partitions %v for table: %s", partitions, table)
	} else {
		g.Info("No partitions like %s found for table: %s", date, table)
		return nil
	}

	for _, partition := range partitions {
		err := dropPartition(conn, table, partition)
		if err != nil {
			g.Error("Failed to drop partition %s from table %s: %v", partition, table, err)
		} else {
			g.Info("Successfully dropped partition %s from table %s", partition, table)
		}
		time.Sleep(1 * time.Second) // Delay between partition drops
	}

	g.Info("Finished cleaning partitions for table: %s", table)
	return nil
}

func getPartitions(conn database.Connection, table, date string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT partition
		FROM system.parts
		WHERE table = '%s'
		  AND partition LIKE '%%%s%%'
	`, table, date)

	data, err := conn.Query(query)
	if err != nil {
		return nil, g.Error(err, "Error querying partitions")
	}

	var partitions []string
	for _, row := range data.Rows {
		partitions = append(partitions, cast.ToString(row[0]))
	}

	return partitions, nil
}

func dropPartition(conn database.Connection, table, partition string) error {
	dropQuery := fmt.Sprintf("ALTER STREAM %s DROP PARTITION %s", table, partition)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := conn.ExecContext(ctx, dropQuery)
	if err != nil {
		return g.Error(err, "Error executing drop partition query")
	}
	return nil
}
