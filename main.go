package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
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
	workerCount = flag.Int(
		"workers",
		10,
		"Number of workers",
	)
)

var ctx = context.Background()

func Write(ctx context.Context, cfg *bt.Config, workerCount int) {
	if err := bt.CreateTableIfNotExists(ctx, cfg); err != nil {
		fmt.Printf("createTableIfNotExists failed: %v\n", err)
		return
	}

	if err := bt.CreateColumnFamiliesIfNotExist(ctx, cfg); err != nil {
		fmt.Printf("createColumnFamiliesIfNotExist failed: %v\n", err)
		return
	}

	workersQueue := make(chan int, workerCount)
	deviceID := rand.Intn(100)
	row := fmt.Sprintf("device/%d/%d", deviceID, time.Now().Unix())
	for {
		workersQueue <- 1

		go func(row string) {
			start := time.Now()

			if _, err := bt.WriteRandomValues(ctx, cfg, row); err != nil {
				fmt.Printf("write failed: %v\n", err)
				return
			}
			elapsed := time.Since(start).Milliseconds()
			fmt.Printf("write successful in %d ms\n", elapsed)
			<-workersQueue
		}(row)
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
		Write(ctx, cfg, *workerCount)
	} else if *runRead {
		fmt.Println("read")
	} else {
		fmt.Println("nothing to do, use --write or --read flag")
	}
}
