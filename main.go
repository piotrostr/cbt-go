package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"bytes"

	"cloud.google.com/go/bigtable"
	"golang.org/x/exp/slices"
)

var ctx = context.Background()

type Config struct {
	projectID  string
	instanceID string
	tableName  string
}

func createTableIfNotExists(ctx context.Context, cfg *Config) error {
	adminClient, err := bigtable.NewAdminClient(ctx, cfg.projectID, cfg.instanceID)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return err
	}

	if len(tables) == 0 || !slices.Contains(tables, cfg.tableName) {
		err = adminClient.CreateTable(ctx, cfg.tableName)
		if err != nil {
			return err
		}
		fmt.Printf("Table %s created successfully \n", cfg.tableName)
	} else {
		fmt.Printf("Table %s already exists\n", cfg.tableName)
	}

	return nil
}

func write(ctx context.Context, cfg *Config) (*string, error) {
	client, err := bigtable.NewClient(ctx, cfg.projectID, cfg.instanceID)
	if err != nil {
		return nil, fmt.Errorf("bigtable.NewClient: %v", err)
	}
	defer client.Close()
	tbl := client.Open(cfg.tableName)
	columnFamilyName := "stats_summary"
	timestamp := bigtable.Now()

	mut := bigtable.NewMutation()
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.BigEndian, int64(1))
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %v", err)
	}

	mut.Set(columnFamilyName, "connected_cell", timestamp, buf.Bytes())
	mut.Set(columnFamilyName, "connected_wifi", timestamp, buf.Bytes())
	mut.Set(columnFamilyName, "os_build", timestamp, []byte("PQ2A.190405.003"))

	rowKey := "phone#4c410523#20190501"
	if err := tbl.Apply(ctx, rowKey, mut); err != nil {
		return nil, fmt.Errorf("Apply: %v", err)
	}

	return &rowKey, nil
}

func main() {
	cfg := &Config{
		projectID:  "piotrostr-resources",
		instanceID: "my-instance-id",
		tableName:  "mobile-time-series",
	}

	if err := createTableIfNotExists(ctx, cfg); err != nil {
		fmt.Printf("createTableIfNotExists failed: %v\n", err)
		return
	}

	rowKey, err := write(ctx, cfg)
	if err != nil {
		fmt.Printf("write failed: %v\n", err)
		return
	}

	fmt.Printf("Successfully wrote row: %s\n", *rowKey)
}
