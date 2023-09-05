package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/kaskada-ai/kaskada/wren/store"
)

type TableService interface {
	SetAnalyzer(analyzer Analyzer)
	ListTables(ctx context.Context, request *v1alpha.ListTablesRequest) (*v1alpha.ListTablesResponse, error)
	GetTable(ctx context.Context, request *v1alpha.GetTableRequest) (*v1alpha.GetTableResponse, error)
	CreateTable(ctx context.Context, request *v1alpha.CreateTableRequest) (*v1alpha.CreateTableResponse, error)
	DeleteTable(ctx context.Context, request *v1alpha.DeleteTableRequest) (*v1alpha.DeleteTableResponse, error)
	LoadData(ctx context.Context, request *v1alpha.LoadDataRequest) (*v1alpha.LoadDataResponse, error)
}

type tableService struct {
	v1alpha.UnimplementedTableServiceServer

	computeManager     compute.ComputeManager
	fileManager        compute.FileManager
	kaskadaTableClient internal.KaskadaTableClient
	objectStoreClient  client.ObjectStoreClient
	tableStore         *store.TableStore
	dependencyAnalyzer Analyzer
}

// NewTableService creates a new table service
func NewTableService(computeManager *compute.ComputeManager, fileManager *compute.FileManager, kaskadaTableClient *internal.KaskadaTableClient, objectStoreClient *client.ObjectStoreClient, tableStore *store.TableStore, dependencyAnalyzer *Analyzer) *tableService {
	return &tableService{
		computeManager:     *computeManager,
		fileManager:        *fileManager,
		kaskadaTableClient: *kaskadaTableClient,
		objectStoreClient:  *objectStoreClient,
		tableStore:         tableStore,
		dependencyAnalyzer: *dependencyAnalyzer,
	}
}

func (t *tableService) ListTables(ctx context.Context, request *v1alpha.ListTablesRequest) (*v1alpha.ListTablesResponse, error) {
	resp, err := t.listTables(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "tableService.ListTables").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (t *tableService) listTables(ctx context.Context, owner *ent.Owner, request *v1alpha.ListTablesRequest) (*v1alpha.ListTablesResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "tableService.listTables").Logger()
	var (
		pageSize, offset int
	)

	if request.PageToken != "" {
		// decode and unmarshal request object from page token
		// to use instead of the initially passed request object
		data, err := base64.URLEncoding.DecodeString(request.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}

		innerRequest := &v1alpha.ListTablesRequest{}

		err = proto.Unmarshal(data, innerRequest)
		if err != nil {
			subLogger.Info().AnErr("unmarshal", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}
		pageSize = int(innerRequest.PageSize)
		offset, err = strconv.Atoi(innerRequest.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}
	} else {
		pageSize = int(request.PageSize)
		offset = 0
	}

	tables, err := t.kaskadaTableClient.ListKaskadaTables(ctx, owner, request.Search, pageSize, offset)
	if err != nil {
		return nil, err
	}

	response := &v1alpha.ListTablesResponse{
		Tables: make([]*v1alpha.Table, 0, len(tables)),
	}
	for _, table := range tables {
		response.Tables = append(response.Tables, t.getProtoFromDB(ctx, table))
	}

	if len(tables) > 0 && len(tables) == pageSize {
		nextRequest := &v1alpha.ListTablesRequest{
			Search:    request.Search,
			PageSize:  int32(pageSize),
			PageToken: strconv.Itoa(offset + pageSize),
		}

		data, err := proto.Marshal(nextRequest)
		if err != nil {
			subLogger.Err(err).Msg("issue listing tables")
			return nil, customerrors.NewInternalError("issue listing tables")
		}

		response.NextPageToken = base64.URLEncoding.EncodeToString(data)
	}

	return response, nil
}

func (t *tableService) GetTable(ctx context.Context, request *v1alpha.GetTableRequest) (*v1alpha.GetTableResponse, error) {
	resp, err := t.getTable(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "table.GetTable").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (t *tableService) getTable(ctx context.Context, owner *ent.Owner, request *v1alpha.GetTableRequest) (*v1alpha.GetTableResponse, error) {
	kaskadaTable, err := t.kaskadaTableClient.GetKaskadaTableByName(ctx, owner, request.TableName)
	if err != nil {
		return nil, err
	}
	return &v1alpha.GetTableResponse{Table: t.getProtoFromDB(ctx, kaskadaTable)}, nil
}

func (t *tableService) CreateTable(ctx context.Context, request *v1alpha.CreateTableRequest) (*v1alpha.CreateTableResponse, error) {
	resp, err := t.createTable(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "table.CreateTable").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (t *tableService) createTable(ctx context.Context, owner *ent.Owner, request *v1alpha.CreateTableRequest) (*v1alpha.CreateTableResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "tableService.createTable").Logger()
	table := request.Table

	// if no table source passed in request, set it to Kaskada source
	if table.Source == nil {
		table.Source = &v1alpha.Source{
			Source: &v1alpha.Source_Kaskada{},
		}
	}

	newTable := &ent.KaskadaTable{
		Name:                table.TableName,
		EntityKeyColumnName: table.EntityKeyColumnName,
		GroupingID:          table.GroupingId,
		TimeColumnName:      table.TimeColumnName,
		Source:              table.Source,
	}

	if table.SubsortColumnName != nil {
		newTable.SubsortColumnName = &request.Table.SubsortColumnName.Value
	}

	switch s := table.Source.Source.(type) {
	case *v1alpha.Source_Kaskada: // if the table source is kaskada, do nothing
	case *v1alpha.Source_Pulsar: // if the table source is pulsar, validate the schema
		streamSchema, err := t.fileManager.GetPulsarSchema(ctx, s.Pulsar.Config)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue getting schema for pulsar")
			return nil, reMapSparrowError(ctx, err)
		}

		err = t.validateSchema(ctx, *newTable, streamSchema)
		if err != nil {
			return nil, err
		}
		newTable.MergedSchema = streamSchema
	case *v1alpha.Source_Kafka:
		subLogger.Log().Msgf("Kafka Source: %v", s)
		streamSchema, err := t.fileManager.GetKafkaSchema(ctx, s.Kafka.Config)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue getting schema for kafka")
			return nil, reMapSparrowError(ctx, err)
		}

		err = t.validateSchema(ctx, *newTable, streamSchema)
		if err != nil {
			return nil, err
		}
		newTable.MergedSchema = streamSchema
	default:
		return nil, customerrors.NewInvalidArgumentError("invalid source type")
	}

	kaskadaTable, err := t.kaskadaTableClient.CreateKaskadaTable(ctx, owner, newTable)
	if err != nil {
		return nil, err
	}

	return &v1alpha.CreateTableResponse{Table: t.getProtoFromDB(ctx, kaskadaTable)}, nil
}

func (t *tableService) DeleteTable(ctx context.Context, request *v1alpha.DeleteTableRequest) (*v1alpha.DeleteTableResponse, error) {
	resp, err := t.deleteTable(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "table.DeleteTable").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (t *tableService) deleteTable(ctx context.Context, owner *ent.Owner, request *v1alpha.DeleteTableRequest) (*v1alpha.DeleteTableResponse, error) {
	kaskadaTable, err := t.kaskadaTableClient.GetKaskadaTableByName(ctx, owner, request.TableName)
	if err != nil {
		return nil, err
	}

	if !request.Force {
		dependencies, err := t.dependencyAnalyzer.Analyze(ctx, owner, request.TableName, schema.DependencyType_Table)
		if err != nil {
			return nil, err
		}

		if len(dependencies.views) > 0 || len(dependencies.materializations) > 0 {
			err := dependencies.ToErrorDetails()
			outStatus := status.Newf(codes.FailedPrecondition, "unable to delete table. detected: %d dependencies.", len(dependencies.views)+len(dependencies.materializations))
			details, _ := outStatus.WithDetails(&err)
			return nil, details.Err()
		}
	}

	newDataToken, err := t.kaskadaTableClient.DeleteKaskadaTable(ctx, owner, kaskadaTable)
	if err != nil {
		return nil, err
	}

	tablePath := t.tableStore.GetTableSubPath(owner, kaskadaTable)
	err = t.objectStoreClient.DeleteObjects(ctx, tablePath)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "table.deleteTable").Str("table_name", kaskadaTable.Name).Str("table_id", kaskadaTable.ID.String()).Logger()
		subLogger.Error().Err(err).Msg("issue deleting files for table")
		return nil, err
	}

	return &v1alpha.DeleteTableResponse{
		DataTokenId: newDataToken.ID.String(),
	}, nil
}

// LoadData loads the data from the staged file to a table
func (t *tableService) LoadData(ctx context.Context, request *v1alpha.LoadDataRequest) (*v1alpha.LoadDataResponse, error) {
	resp, err := t.loadData(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "table.LoadData").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (t *tableService) loadData(ctx context.Context, owner *ent.Owner, request *v1alpha.LoadDataRequest) (*v1alpha.LoadDataResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "table.loadData").Logger()

	kaskadaTable, err := t.kaskadaTableClient.GetKaskadaTableByName(ctx, owner, request.TableName)
	if err != nil {
		return nil, err
	}

	var newDataToken *ent.DataToken

	switch r := request.SourceData.(type) {
	case *v1alpha.LoadDataRequest_FileInput:
		fileInput := internal.FileInputFromV1Alpha(r.FileInput)
		newDataToken, err = t.loadFileIntoTable(ctx, owner, fileInput, kaskadaTable, request.CopyToFilesystem)
		if err != nil {
			return nil, err
		}
	default:
		subLogger.Error().Interface("type", r).Msg("load data request type not implemented")
		return nil, fmt.Errorf("load data request type %s is not implemented", r)
	}
	if err != nil {
		return nil, err
	}

	go t.computeManager.RunMaterializations(ctx, owner)

	return &v1alpha.LoadDataResponse{
		DataTokenId: newDataToken.ID.String(),
	}, nil
}

func (t *tableService) loadFileIntoTable(ctx context.Context, owner *ent.Owner, fileInput internal.FileInput, kaskadaTable *ent.KaskadaTable, copyToFilesystem bool) (*ent.DataToken, error) {
	subLogger := log.Ctx(ctx).With().
		Str("method", "table.loadFileIntoTable").
		Str("table_name", kaskadaTable.Name).
		Str("table_id", kaskadaTable.ID.String()).
		Str("file_uri", fileInput.GetURI()).
		Logger()

	// first validate we can access the file
	exists, err := t.objectStoreClient.URIExists(ctx, fileInput.GetURI())
	if err != nil {
		subLogger.Info().Err(err).Msg("issue checking if file exists")
		return nil, customerrors.NewPermissionDeniedError(fmt.Sprintf("file: %s is not accessible by the kaskada service. The provider returned this error: %s", fileInput.GetURI(), err.Error()))
	}
	if !exists {
		return nil, customerrors.NewNotFoundErrorWithCustomText(fmt.Sprintf("file: %s not found by the kaskada service", fileInput.GetURI()))
	}

	fileSchema, err := t.fileManager.GetFileSchema(ctx, fileInput)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting schema for file")
		return nil, reMapSparrowError(ctx, err)
	}

	err = t.validateSchema(ctx, *kaskadaTable, fileSchema)
	if err != nil {
		return nil, err
	}

	var cleanupOnError func() error
	var newFileURI string
	if copyToFilesystem {
		toPath := t.tableStore.GetFileSubPath(owner, kaskadaTable, fileInput.GetExtension())
		newObject, err := t.objectStoreClient.CopyObjectIn(ctx, fileInput.GetURI(), toPath)
		if err != nil {
			subLogger.Error().Err(err).Msg("issue copying file into table")
			return nil, err
		}
		newFileURI = newObject.URI()

		cleanupOnError = func() error {
			return t.objectStoreClient.DeleteObject(ctx, newObject)
		}
	} else {
		newFileURI = fileInput.GetURI()

		cleanupOnError = func() error { return nil }

	}

	fileIdentifier, err := t.objectStoreClient.GetObjectIdentifier(ctx, newFileURI)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting identifier for file")
		return nil, err
	}

	newFiles := []internal.AddFileProps{}
	newFiles = append(newFiles, internal.AddFileProps{
		URI:        newFileURI,
		Identifier: *fileIdentifier,
		Schema:     fileSchema,
		FileType:   fileInput.GetType(),
	})

	newDataToken, err := t.kaskadaTableClient.AddFilesToTable(ctx, owner, kaskadaTable, newFiles, fileSchema, nil, cleanupOnError)
	if err != nil {
		return nil, err
	}
	return newDataToken, nil
}

// validateSchema performs the validation of the a file vs the desired table
// assumes that the provided table and schema are up to date and valid
// TODO: return all the return all the potential issues in a single response.  Similar to how the request validation works.
func (t *tableService) validateSchema(ctx context.Context, kaskadaTable ent.KaskadaTable, schema *v1alpha.Schema) error {
	subLogger := log.Ctx(ctx).With().Str("method", "table.validateSchema").Logger()

	if kaskadaTable.MergedSchema != nil && !proto.Equal(kaskadaTable.MergedSchema, schema) {
		subLogger.Warn().Interface("table_schema", kaskadaTable.MergedSchema).Interface("schema", schema).Msg("new schema doesn't match schema of table")
		return customerrors.NewFailedPreconditionError("schema does not match previous schema")
	}

	columnNames := map[string]interface{}{}
	for _, field := range schema.Fields {
		columnNames[field.Name] = nil
	}

	if _, ok := columnNames[kaskadaTable.EntityKeyColumnName]; !ok {
		return customerrors.NewFailedPreconditionError(fmt.Sprintf("schema does not contain entity key column: '%s'", kaskadaTable.EntityKeyColumnName))
	}

	if kaskadaTable.SubsortColumnName != nil {
		if _, ok := columnNames[*kaskadaTable.SubsortColumnName]; !ok {
			return customerrors.NewFailedPreconditionError(fmt.Sprintf("schema does not contain subsort column: '%s'", *kaskadaTable.SubsortColumnName))
		}
	}
	if _, ok := columnNames[kaskadaTable.TimeColumnName]; !ok {
		return customerrors.NewFailedPreconditionError(fmt.Sprintf("schema does not contain time column: '%s'", kaskadaTable.TimeColumnName))
	}
	return nil
}

func reMapSparrowError(ctx context.Context, err error) error {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.reMapSparrowError").Logger()
	subLogger.Info().Msg(fmt.Sprintf("Sparrow Error: %v", err))

	inStatus, ok := status.FromError(err)
	if !ok {
		subLogger.Error().Msg("unexpected: compute error did not include error-status")
		return err
	}
	outStatus := status.New(inStatus.Code(), inStatus.Message())
	subLogger.Info().Msg(fmt.Sprintf("Error Out Status: %v", outStatus))

	for _, detail := range inStatus.Details() {
		subLogger.Info().Msg(fmt.Sprintf("Error Detail: %v", detail))
		switch t := detail.(type) {
		case protoiface.MessageV1:
			outStatus, err = outStatus.WithDetails(t)
			subLogger.Info().Msg(fmt.Sprintf("Error Out Status: %v", outStatus))
			if err != nil {
				subLogger.Error().Err(err).Interface("detail", t).Msg("unable to add detail to re-mapped error details")
			}
		default:
			subLogger.Error().Err(err).Interface("detail", t).Msg("unexpected: detail from compute doesn't implement the protoifam.MessageV1 interface")
		}
	}
	return outStatus.Err()
}

func (t *tableService) getProtoFromDB(ctx context.Context, table *ent.KaskadaTable) *v1alpha.Table {
	subLogger := log.Ctx(ctx).With().Str("method", "table.getProtoFromDB").Str("table_name", table.Name).Str("table_id", table.ID.String()).Logger()

	output := &v1alpha.Table{
		TableId:             table.ID.String(),
		TableName:           table.Name,
		TimeColumnName:      table.TimeColumnName,
		EntityKeyColumnName: table.EntityKeyColumnName,
		GroupingId:          table.GroupingID,
		CreateTime:          timestamppb.New(table.CreatedAt),
		UpdateTime:          timestamppb.New(table.UpdatedAt),
		Schema:              table.MergedSchema,
		Source:              table.Source,
	}

	version, err := t.kaskadaTableClient.GetKaskadaTableVersion(ctx, table)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting table version")
	}
	if version != nil {
		output.Version = version.ID
	}

	if table.SubsortColumnName != nil {
		output.SubsortColumnName = &wrapperspb.StringValue{
			Value: *table.SubsortColumnName,
		}
	}

	return output
}
