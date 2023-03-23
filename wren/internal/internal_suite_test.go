package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"entgo.io/ent/dialect/sql/schema"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/kaskadafile"
	"github.com/kaskada-ai/kaskada/wren/ent/migrate"
	_ "github.com/kaskada-ai/kaskada/wren/ent/runtime"
)

var (
	entClient *ent.Client
	ctx       context.Context
)

func TestInternal(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: GinkgoWriter})

	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal Suite")
}

var _ = BeforeSuite(func() {
	ctx = context.Background()
	inMemory := true
	entConfig := client.NewEntConfig("sqlite", nil, nil, &inMemory, nil, nil, nil, nil, nil)
	entClient = client.NewEntClient(ctx, entConfig)
	Expect(entClient).ShouldNot(BeNil())

	DeferCleanup(func() {
		err := entClient.Close()
		Expect(err).ShouldNot(HaveOccurred())
	})
})

func getOwner() *ent.Owner {
	ownerClient := NewOwnerClient(entClient)
	Expect(ownerClient).ShouldNot(BeNil())

	//create dummy first owner, so that more than 1 owner exists when querying owners
	initialOwner, err := ownerClient.GetOwnerFromClientID(ctx, "initial_client_id")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(initialOwner).ShouldNot(BeNil())

	owner, err := ownerClient.GetOwnerFromClientID(ctx, "internal_test_client_id")
	Expect(err).ShouldNot(HaveOccurred())
	Expect(owner).ShouldNot(BeNil())
	return owner
}

func testCreateTable(tableClient KaskadaTableClient, owner *ent.Owner, tableName string) *ent.KaskadaTable {
	table, err := tableClient.CreateKaskadaTable(ctx, owner, &ent.KaskadaTable{
		Name:                tableName,
		EntityKeyColumnName: fmt.Sprintf("%s_key", tableName),
		TimeColumnName:      fmt.Sprintf("%s_time", tableName),
		Source:              &v1alpha.Source{Source: &v1alpha.Source_Kaskada{}},
	})

	Expect(err).ShouldNot(HaveOccurred())
	Expect(table).ShouldNot(BeNil())

	Expect(table.QueryOwner().FirstID(ctx)).Should(Equal(owner.ID))
	return table
}

func testDeleteTable(tableClient KaskadaTableClient, owner *ent.Owner, table *ent.KaskadaTable) *ent.DataToken {
	tableID := table.ID
	dataToken, err := tableClient.DeleteKaskadaTable(ctx, owner, table)

	Expect(err).ShouldNot(HaveOccurred())
	Expect(dataToken).ShouldNot(BeNil())

	Expect(dataToken.QueryOwner().FirstID(ctx)).Should(Equal(owner.ID))
	Expect(dataToken.KaskadaTableID).Should(Equal(tableID))
	return dataToken
}

func emptyCleanupFunc() error {
	return nil
}

func testAddingFilesToTable(tableClient KaskadaTableClient, owner *ent.Owner, files []string, table *ent.KaskadaTable) *ent.DataToken {
	newFiles := []AddFileProps{}

	for _, file := range files {
		newFiles = append(newFiles, AddFileProps{
			URI:        "s3://bucket/file",
			Identifier: file,
			Schema:     nil,
			FileType:   kaskadafile.TypeParquet,
		})
	}

	dataToken, err := tableClient.AddFilesToTable(ctx, owner, table, newFiles, nil, nil, emptyCleanupFunc)
	Expect(err).ShouldNot(HaveOccurred())

	Expect(dataToken).ShouldNot(BeNil())
	Expect(dataToken.QueryOwner().FirstID(ctx)).Should(Equal(owner.ID))
	Expect(dataToken.KaskadaTableID).Should(Equal(table.ID))
	return dataToken
}

func getTime(dateTime string) time.Time {
	t, err := time.Parse(time.RFC3339, dateTime)
	Expect(err).ShouldNot(HaveOccurred())
	return t
}

func getProtoTimestamp(dateTime string) *timestamppb.Timestamp {
	return timestamppb.New(getTime(dateTime))
}

// below based on ent/enttest/enttest.go but modified for use with ginkgo

type (
	// EntOption configures entClient creation.
	EntOption func(*entOptions)

	entOptions struct {
		opts        []ent.Option
		migrateOpts []schema.MigrateOption
	}
)

// WithOptions forwards options to client creation.
func WithOptions(opts ...ent.Option) EntOption {
	return func(o *entOptions) {
		o.opts = append(o.opts, opts...)
	}
}

// WithMigrateOptions forwards options to auto migration.
func WithMigrateOptions(opts ...schema.MigrateOption) EntOption {
	return func(o *entOptions) {
		o.migrateOpts = append(o.migrateOpts, opts...)
	}
}

func newOptions(opts []EntOption) *entOptions {
	o := &entOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Open calls ent.Open and auto-run migration.
func entOpen(ctx context.Context, driverName, dataSourceName string, opts ...EntOption) *ent.Client {
	o := newOptions(opts)
	c, err := ent.Open(driverName, dataSourceName, o.opts...)
	Expect(err).ShouldNot(HaveOccurred())
	entMigrateSchema(ctx, c, o)
	return c
}

func entMigrateSchema(ctx context.Context, c *ent.Client, o *entOptions) {
	tables, err := schema.CopyTables(migrate.Tables)
	Expect(err).ShouldNot(HaveOccurred())
	err = migrate.Create(ctx, c.Schema, tables, o.migrateOpts...)
	Expect(err).ShouldNot(HaveOccurred())
}
