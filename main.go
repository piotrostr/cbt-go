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
		"iot-example-instance",
		"The Cloud Bigtable instance ID",
	)
	tableName = flag.String(
		"table",
		"random-time-series",
		"The Cloud Bigtable table ID",
	)
	columnFamilyName = flag.String(
		"column-family",
		"cf",
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
		100,
		"Number of workers",
	)
)

var ctx = context.Background()

func Write(ctx context.Context, cfg *bt.Config, workerCount int) {
	if err := bt.CreateTableIfNotExists(ctx, cfg); err != nil {
		fmt.Printf("CreateTableIfNotExists failed: %v\n", err)
		return
	}

	if err := bt.CreateColumnFamiliesIfNotExist(ctx, cfg); err != nil {
		fmt.Printf("CreateColumnFamiliesIfNotExist failed: %v\n", err)
		return
	}

	workersQueue := make(chan int, workerCount)
	rand.Seed(time.Now().UnixNano())
	deviceID := rand.Intn(100)
	for {
		workersQueue <- 1

		go func() {
			row := fmt.Sprintf("device/%d/%d", deviceID, time.Now().UnixNano())

			start := time.Now()
			if _, err := bt.WriteRandomValues(ctx, cfg, row); err != nil {
				fmt.Printf("Write failed: %v\n", err)
				return
			}
			elapsed := time.Since(start).Milliseconds()

			fmt.Printf("Write successful %s (%d ms)\n", row, elapsed)

			<-workersQueue
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
		Write(ctx, cfg, *workerCount)
	} else if *runRead {
		err := bt.ReadFromTable(ctx, cfg)
		if err != nil {
			fmt.Printf("Read failed: %v\n", err)
			return
		}
	} else {
		fmt.Println("Nothing to do, use --write or --read flag")
	}
}
