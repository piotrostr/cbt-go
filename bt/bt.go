package bt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

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
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.BigEndian, int64(1))
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %v", err)
	}

	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_1",
		timestamp,
		[]byte(RandomString(3000)),
	)
	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_2",
		timestamp,
		[]byte(RandomString(3000)),
	)
	mut.Set(
		cfg.ColumnFamilyName,
		"some_random_value_3",
		timestamp,
		[]byte(RandomString(3000)),
	)

	if err := tbl.Apply(ctx, row, mut); err != nil {
		return nil, fmt.Errorf("Apply: %v", err)
	}

	return &row, nil
}

func ReadFromTable(ctx context.Context, cfg *Config) error {
	client, err := bigtable.NewClient(ctx, cfg.ProjectID, cfg.InstanceID)
	if err != nil {
		return fmt.Errorf("bigtable.NewClient: %v", err)
	}
	defer client.Close()

	tbl := client.Open(cfg.TableName)
	err = tbl.ReadRows(
		ctx,
		bigtable.PrefixRange("random#"),
		func(r bigtable.Row) bool {
			fmt.Println(r)
			return true
		},
	)
	if err != nil {
		return fmt.Errorf("ReadRow: %v", err)
	}

	return nil
}

func RandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}
