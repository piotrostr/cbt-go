package bt

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"time"

	pretty "github.com/jedib0t/go-pretty/v6/table"

	"cloud.google.com/go/bigtable"
	"github.com/piotrostr/logger"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type Config struct {
	ProjectID        string
	InstanceID       string
	TableName        string
	ColumnFamilyName string
}

var log = logger.NewLogger()

func CreateTableIfNotExists(ctx context.Context, cfg *Config) error {
	adminClient, err := bigtable.NewAdminClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return err
	}

	if !slices.Contains(tables, cfg.TableName) {
		err = adminClient.CreateTable(ctx, cfg.TableName)
		if err != nil {
			return err
		}

		log.Info(
			"Table created successfully",
			zap.String("table-name", cfg.TableName),
		)
	} else {
		log.Info(
			"Table created successfully",
			zap.String("table-name", cfg.TableName),
		)
	}

	return nil
}

func CreateColumnFamiliesIfNotExist(ctx context.Context, cfg *Config) error {
	adminClient, err := bigtable.NewAdminClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	tblInfo, err := adminClient.TableInfo(ctx, cfg.TableName)
	if err != nil {
		return err
	}

	columnFamilyNames := make([]string, len(tblInfo.FamilyInfos))
	for _, entry := range tblInfo.FamilyInfos {
		columnFamilyNames = append(columnFamilyNames, entry.Name)
	}

	if !slices.Contains(columnFamilyNames, cfg.ColumnFamilyName) {
		err = adminClient.CreateColumnFamily(
			ctx,
			cfg.TableName,
			cfg.ColumnFamilyName,
		)
		if err != nil {
			return err
		}
		log.Info(
			"Column family created successfully",
			zap.String("column-family-name", cfg.ColumnFamilyName),
		)
	} else {
		log.Info(
			"Column family already exists",
			zap.String("column-family-name", cfg.ColumnFamilyName),
		)
	}
	return nil
}

func WriteRandomValues(
	ctx context.Context,
	cfg *Config,
	row string,
) error {
	client, err := bigtable.NewClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		return err
	}
	defer client.Close()
	tbl := client.Open(cfg.TableName)
	timestamp := bigtable.Now()

	mut := bigtable.NewMutation()

	var rbytes []byte
	rbytes, err = RandomFloatBytes()
	if err != nil {
		return err
	}
	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_1",
		timestamp,
		rbytes,
	)

	rbytes, err = RandomFloatBytes()
	if err != nil {
		return err
	}
	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_2",
		timestamp,
		rbytes,
	)

	rbytes, err = RandomFloatBytes()
	if err != nil {
		return err
	}
	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_3",
		timestamp,
		rbytes,
	)

	if err := tbl.Apply(ctx, row, mut); err != nil {
		return err
	}

	return nil
}

func ReadBasedOnPrefix(ctx context.Context, cfg *Config, prefix string) error {
	client, err := bigtable.NewClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		return err
	}
	defer client.Close()

	tbl := client.Open(cfg.TableName)
	opts := []bigtable.ReadOption{
		bigtable.LimitRows(50),
	}
	t := pretty.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(pretty.Row{"Row Key", "Column", "Value"})
	start := time.Now()
	err = tbl.ReadRows(
		ctx,
		bigtable.PrefixRange(prefix),
		func(r bigtable.Row) bool {
			rowKeyAppended := false
			for _, column := range r[cfg.ColumnFamilyName] {
				if !rowKeyAppended {
					t.AppendRow(pretty.Row{
						r.Key(),
						column.Column,
						"0x" + hex.EncodeToString(column.Value),
					})
					rowKeyAppended = true
				} else {
					t.AppendRow(pretty.Row{
						"",
						column.Column,
						"0x" + hex.EncodeToString(column.Value),
					})
				}
			}
			return true
		},
		opts...,
	)
	if err != nil {
		return err
	}
	elapsed := time.Since(start).Milliseconds()

	t.Render()
	log.Info(
		"Time elapsed reading",
		zap.Int64("ms", elapsed),
	)

	return nil
}

func RandomString(length int) (string, error) {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b)[:length], nil
}

func RandomFloatBytes() ([]byte, error) {
	rand.Seed(time.Now().UnixNano())
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, rand.Float64())
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
