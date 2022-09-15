package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/piotrostr/cbt-go/bt"
	"github.com/piotrostr/logger"
	"go.uber.org/zap"
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
	prefix = flag.String(
		"prefix",
		"device/26",
		"Prefix to read",
	)
)

var ctx = context.Background()

var log = logger.NewLogger()

func Write(ctx context.Context, cfg *bt.Config, workerCount int) error {
	if err := bt.CreateTableIfNotExists(ctx, cfg); err != nil {
		return err
	}

	if err := bt.CreateColumnFamiliesIfNotExist(ctx, cfg); err != nil {
		log.Error(err.Error())
		return err
	}

	workersQueue := make(chan int, workerCount)
	errch := make(chan error, workerCount)
	rand.Seed(time.Now().UnixNano())
	deviceID := rand.Intn(100)
	for {
		workersQueue <- 1

		select {
		case err := <-errch:
			return err
		default:
			go func() {
				row := fmt.Sprintf("device/%d/%d", deviceID, time.Now().UnixNano())

				start := time.Now()
				if err := bt.WriteRandomValues(ctx, cfg, row); err != nil {
					errch <- err
					return
				}
				elapsed := time.Since(start).Milliseconds()

				log.Info(
					"Write successful",
					zap.String("row", row),
					zap.Int64("elapsed", elapsed),
				)

				<-workersQueue
			}()
		}
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

	log.Info(
		"Config",
		zap.String("project", cfg.ProjectID),
		zap.String("instance", cfg.InstanceID),
		zap.String("table", cfg.TableName),
		zap.String("column-family", cfg.ColumnFamilyName),
	)

	if *runWrite {
		err := Write(ctx, cfg, *workerCount)
		if err != nil {
			log.Fatal("Write", zap.Error(err))
		}
	} else if *runRead {
		err := bt.ReadBasedOnPrefix(
			ctx,
			cfg,
			*prefix,
		)
		if err != nil {
			log.Fatal("ReadBasedOnPrefix", zap.Error(err))
			return
		}
	} else {
		log.Info("Nothing to do, use --write or --read flag\n")
	}
}
