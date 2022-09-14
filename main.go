package main

import (
	"context"
	"fmt"
	"time"

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
			elapsed := time.Since(start).Abs().Milliseconds()
			fmt.Printf("write successful in %d ms\n", elapsed)
			<-devices
		}()
	}
}
