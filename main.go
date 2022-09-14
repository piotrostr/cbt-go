package main

import (
	"context"
	"fmt"
	"sync"

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

	// lets create fake 100 IOT devices, sending data to BigTable every second
	deviceCount := 50
	var wg sync.WaitGroup
	for i := 1; i < deviceCount; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			if rowKey, err := bt.WriteRandomValues(ctx, cfg); err != nil {
				fmt.Printf("device %d: write failed: %v\n", i, err)
				return
			} else {
				fmt.Printf("device %d: write successful row: %s\n", i, *rowKey)
			}
		}(&wg, i)
	}
	wg.Wait()
}
