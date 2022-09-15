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
	"golang.org/x/exp/slices"
)

type Config struct {
	ProjectID        string
	InstanceID       string
	TableName        string
	ColumnFamilyName string
}

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
		fmt.Printf("Table %s created successfully \n", cfg.TableName)
	} else {
		fmt.Printf("Table %s already exists\n", cfg.TableName)
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
		fmt.Printf("Column family %s created successfully\n", cfg.ColumnFamilyName)
	} else {
		fmt.Printf("Column family %s already exists\n", cfg.ColumnFamilyName)
	}
	return nil
}

func WriteRandomValues(ctx context.Context, cfg *Config, row string) (*string, error) {
	client, err := bigtable.NewClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("bigtable.NewClient: %v", err)
	}
	defer client.Close()
	tbl := client.Open(cfg.TableName)
	timestamp := bigtable.Now()

	mut := bigtable.NewMutation()

	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_1",
		timestamp,
		RandomFloatBytes(),
	)
	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_2",
		timestamp,
		RandomFloatBytes(),
	)
	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_3",
		timestamp,
		RandomFloatBytes(),
	)

	if err := tbl.Apply(ctx, row, mut); err != nil {
		return nil, fmt.Errorf("Apply: %v", err)
	}

	return &row, nil
}

func ReadBasedOnPrefix(ctx context.Context, cfg *Config, prefix string) error {
	client, err := bigtable.NewClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		return fmt.Errorf("bigtable.NewClient: %v", err)
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
		return fmt.Errorf("ReadRow: %v", err)
	}
	elapsed := time.Since(start).Milliseconds()

	t.Render()
	fmt.Printf("Elapsed: %d ms\n", elapsed)

	return nil
}

func RandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}

func RandomFloatBytes() []byte {
	rand.Seed(time.Now().UnixNano())
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, rand.Float64())
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}
