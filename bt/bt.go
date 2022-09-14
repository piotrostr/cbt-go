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
		err = adminClient.CreateColumnFamily(ctx, cfg.TableName, "stats_summary")
		if err != nil {
			return err
		}
		fmt.Printf("Column family stats_summary created successfully \n")
	} else {
		fmt.Printf("Column family stats_summary already exists\n")
	}
	return nil
}

func WriteRandomValues(ctx context.Context, cfg *Config) (*string, error) {
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

	mut.Set(cfg.ColumnFamilyName, "connected_cell", timestamp, buf.Bytes())
	mut.Set(cfg.ColumnFamilyName, "connected_wifi", timestamp, buf.Bytes())
	mut.Set(cfg.ColumnFamilyName, "os_build", timestamp, []byte("PQ2A.190405.003"))
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

	rowKey := fmt.Sprintf("phone#%s#%s", RandomString(16), RandomString(16))
	if err := tbl.Apply(ctx, rowKey, mut); err != nil {
		return nil, fmt.Errorf("Apply: %v", err)
	}

	return &rowKey, nil
}

func RandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}
