package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/piotrostr/cbt-go/bt"
)

var (
	projectID = flag.String(
		"project",
		"piotrostr-resources",
		"The Google Cloud project ID",
	)
	instanceID = flag.String(
		"instance",
		"my-instance-id",
		"The Cloud Bigtable instance ID",
	)
	tableName = flag.String(
		"table",
		"mobile-time-series",
		"The Cloud Bigtable table ID",
	)
	columnFamilyName = flag.String(
		"column-family",
		"stats_summary",
		"The Cloud Bigtable column family ID",
	)
	runWrite = flag.Bool(
		"write",
		false,
		"Run write example",
	)
	runRead = flag.Bool(
		"read",
		false,
		"Run read example",
	)
)

var ctx = context.Background()

func Write(ctx context.Context, cfg *bt.Config) {
	if err := bt.CreateTableIfNotExists(ctx, cfg); err != nil {
		fmt.Printf("createTableIfNotExists failed: %v\n", err)
		return
	}

	if err := bt.CreateColumnFamiliesIfNotExist(ctx, cfg); err != nil {
		fmt.Printf("createColumnFamiliesIfNotExist failed: %v\n", err)
		return
	}

	deviceCount := 100
	devices := make(chan int, deviceCount)
	for {
		devices <- 1
		go func() {
			start := time.Now()
			if _, err := bt.WriteRandomValues(ctx, cfg); err != nil {
				fmt.Printf("write failed: %v\n", err)
				return
			}
			elapsed := time.Since(start).Milliseconds()
			fmt.Printf("write successful in %d ms\n", elapsed)
			<-devices
		}()
	}

}

func main() {
	flag.Parse()

	cfg := &bt.Config{
		ProjectID:        *projectID,
		InstanceID:       *instanceID,
		TableName:        *tableName,
		ColumnFamilyName: *columnFamilyName,
	}

	if *runWrite {
		Write(ctx, cfg)
	} else if *runRead {
		fmt.Println("read")
	} else {
		fmt.Println("nothing to do, use --write or --read flag")
	}
}
