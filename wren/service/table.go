package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

	computeManager     *compute.Manager
	kaskadaTableClient internal.KaskadaTableClient
	objectStoreClient  client.ObjectStoreClient
	tableStore         *store.TableStore
	dependencyAnalyzer Analyzer
}

// NewTableService creates a new table service
func NewTableService(computeManager *compute.Manager, kaskadaTableClient *internal.KaskadaTableClient, objectStoreClient *client.ObjectStoreClient, tableStore *store.TableStore, dependencyAnalyzer *Analyzer) *tableService {
	return &tableService{
		computeManager:     computeManager,
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
		protoTable, err := t.getProtoFromDB(ctx, table)
		if err != nil {
			return nil, errors.WithMessage(err, "coverting db table to proto table")
		}

		response.Tables = append(response.Tables, protoTable)
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
	table, err := t.getProtoFromDB(ctx, kaskadaTable)
	if err != nil {
		return nil, errors.WithMessage(err, "getting proto from db table")
	}
	return &v1alpha.GetTableResponse{Table: table}, nil
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
	table := request.Table

	// if no table source passed in request, set it to Kaskada source
	if table.TableSource == nil {
		table.TableSource = &v1alpha.Table_TableSource{
			Source: &v1alpha.Table_TableSource_Kaskada{},
		}
	}

	newTable := &ent.KaskadaTable{
		Name:                table.TableName,
		EntityKeyColumnName: table.EntityKeyColumnName,
		GroupingID:          table.GroupingId,
		TimeColumnName:      table.TimeColumnName,
		Source:              table.TableSource,
	}

	if table.SubsortColumnName != nil {
		newTable.SubsortColumnName = &request.Table.SubsortColumnName.Value
	}

	kaskadaTable, err := t.kaskadaTableClient.CreateKaskadaTable(ctx, owner, newTable)
	if err != nil {
		return nil, err
	}

	table, err = t.getProtoFromDB(ctx, kaskadaTable)
	if err != nil {
		return nil, errors.WithMessage(err, "getting proto from db table")
	}

	return &v1alpha.CreateTableResponse{Table: table}, nil
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
		return nil, errors.WithMessagef(err, "deleting files for table: %s", request.TableName)
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
		newDataToken, err = t.loadFileIntoTable(ctx, owner, fileInput, kaskadaTable)
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

func (t *tableService) loadFileIntoTable(ctx context.Context, owner *ent.Owner, fileInput internal.FileInput, kaskadaTable *ent.KaskadaTable) (*ent.DataToken, error) {
	//subLogger := log.Ctx(ctx).With().Str("method", "table.loadFileIntoTable").Logger()

	fileSchema, err := t.validateFileSchema(ctx, *kaskadaTable, fileInput)
	if err != nil {
		return nil, errors.WithMessagef(err, "validating schema for file: %s on table: %s", fileInput, kaskadaTable.Name)
	}
	toPath := t.tableStore.GetFileSubPath(owner, kaskadaTable, fileInput.GetExtension())

	newObject, err := t.objectStoreClient.CopyObjectIn(ctx, fileInput.GetURI(), toPath)
	if err != nil {
		return nil, errors.WithMessagef(err, "loading file: %s into table: %s", fileInput, kaskadaTable.Name)
	}

	fileIdentifier, err := t.objectStoreClient.GetObjectIdentifier(ctx, newObject)
	if err != nil {
		return nil, errors.WithMessagef(err, "getting identifier for file: %s in table: %s", fileInput, kaskadaTable.Name)
	}

	newFiles := []internal.AddFileProps{}

	newFiles = append(newFiles, internal.AddFileProps{
		URI:        newObject.URI(),
		Identifier: fileIdentifier,
		Schema:     fileSchema,
		FileType:   fileInput.GetType(),
	})

	cleanupOnError := func() error {
		return t.objectStoreClient.DeleteObject(ctx, newObject)
	}

	newDataToken, err := t.kaskadaTableClient.AddFilesToTable(ctx, owner, kaskadaTable, newFiles, fileSchema, nil, cleanupOnError)
	if err != nil {
		return nil, err
	}
	return newDataToken, nil
}

// validateFileSchema performs the validation of the a file vs the desired table
// assumes that the provided table and schema are up to date and valid
// TODO: return all the return all the potential issues in a single response.  Similar to how the request validation works.
func (t *tableService) validateFileSchema(ctx context.Context, kaskadaTable ent.KaskadaTable, fileInput internal.FileInput) (*v1alpha.Schema, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "table.validateFileSchema").Logger()

	fileSchema, err := t.computeManager.GetSourceSchema(ctx, fileInput)
	if err != nil {
		return nil, errors.WithMessagef(err, "getting schema for file: %s", fileInput)
	}

	if kaskadaTable.MergedSchema != nil && !proto.Equal(kaskadaTable.MergedSchema, fileSchema) {
		subLogger.Warn().Interface("table_schema", kaskadaTable.MergedSchema).Interface("file_schema", fileSchema).Str("file_uri", fileInput.GetURI()).Msg("new file doesn't match schema of table")
		return nil, customerrors.NewFailedPreconditionError("file schema does not match previous files")
	}

	columnNames := map[string]interface{}{}
	for _, field := range fileSchema.Fields {
		columnNames[field.Name] = nil
	}

	if _, ok := columnNames[kaskadaTable.EntityKeyColumnName]; !ok {
		return nil, customerrors.NewFailedPreconditionError(fmt.Sprintf("file does not contain entity key column: '%s'", kaskadaTable.EntityKeyColumnName))
	}

	if kaskadaTable.SubsortColumnName != nil {
		if _, ok := columnNames[*kaskadaTable.SubsortColumnName]; !ok {
			return nil, customerrors.NewFailedPreconditionError(fmt.Sprintf("file does not contain subsort column: '%s'", *kaskadaTable.SubsortColumnName))
		}
	}
	if _, ok := columnNames[kaskadaTable.TimeColumnName]; !ok {
		return nil, customerrors.NewFailedPreconditionError(fmt.Sprintf("file does not contain time column: '%s'", kaskadaTable.TimeColumnName))
	}
	return fileSchema, nil
}

func (t *tableService) getProtoFromDB(ctx context.Context, table *ent.KaskadaTable) (*v1alpha.Table, error) {
	output := &v1alpha.Table{
		TableId:             table.ID.String(),
		TableName:           table.Name,
		TimeColumnName:      table.TimeColumnName,
		EntityKeyColumnName: table.EntityKeyColumnName,
		GroupingId:          table.GroupingID,
		CreateTime:          timestamppb.New(table.CreatedAt),
		UpdateTime:          timestamppb.New(table.UpdatedAt),
		Schema:              table.MergedSchema,
		TableSource:         table.Source,
	}

	version, err := t.kaskadaTableClient.GetKaskadaTableVersion(ctx, table)
	if err != nil {
		return nil, err
	}
	if version != nil {
		output.Version = version.ID
	}

	if table.SubsortColumnName != nil {
		output.SubsortColumnName = &wrapperspb.StringValue{
			Value: *table.SubsortColumnName,
		}
	}

	return output, nil
}
