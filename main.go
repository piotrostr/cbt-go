package main

import (
	"context"
	"fmt"

	"github.com/piotrostr/cbt-go/bt"
)

var ctx = context.Background()

func main() {
	cfg := &bt.Config{
		ProjectID:        "piotrostr-resources",
		InstanceID:       "my-instance-id",
		TableName:        "mobile-time-series",
		ColumnFamilyName: "stats_summary",
	}

	if err := bt.CreateTableIfNotExists(ctx, cfg); err != nil {
		fmt.Printf("createTableIfNotExists failed: %v\n", err)
		return
	}

	if err := bt.CreateColumnFamiliesIfNotExist(ctx, cfg); err != nil {
		fmt.Printf("createColumnFamiliesIfNotExist failed: %v\n", err)
		return
	}

	if rowKey, err := bt.Write(ctx, cfg); err != nil {
		fmt.Printf("write failed: %v\n", err)
		return
	} else {
		fmt.Printf("write successful row: %s\n", *rowKey)
	}
}
