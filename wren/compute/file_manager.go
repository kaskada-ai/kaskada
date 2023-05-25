package compute

import (
	"context"
	"fmt"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/ent/kaskadafile"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/rs/zerolog/log"
)

type FileManager interface {
	// metadata related
	GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error)
}

type fileManager struct {
	computeClients client.ComputeClients
}

func NewFileManager(computeClients *client.ComputeClients) FileManager {
	return &fileManager{
		computeClients: *computeClients,
	}
}

func (m *fileManager) GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.GetFileSchema").Str("uri", fileInput.GetURI()).Str("type", fileInput.GetExtension()).Logger()
	// Send the metadata request to the FileService

	var sourceData *v1alpha.SourceData

	switch fileInput.GetType() {
	case kaskadafile.TypeCsv:
		sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_CsvPath{CsvPath: fileInput.GetURI()}}
	case kaskadafile.TypeParquet:
		sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_ParquetPath{ParquetPath: fileInput.GetURI()}}
	default:
		subLogger.Warn().Msg("user didn't specifiy file type, defaulting to parquet for now, but will error in the future")
		sourceData = &v1alpha.SourceData{Source: &v1alpha.SourceData_ParquetPath{ParquetPath: fileInput.GetURI()}}
	}

	fileClient := m.computeClients.NewFileServiceClient(ctx)
	defer fileClient.Close()

	metadataReq := &v1alpha.GetMetadataRequest{
		SourceData: sourceData,
	}

	subLogger.Debug().Interface("request", metadataReq).Msg("sending get_metadata request to file service")
	metadataRes, err := fileClient.GetMetadata(ctx, metadataReq)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue getting file schema from file_service")
		return nil, err
	}

	if metadataRes.SourceMetadata == nil {
		subLogger.Error().Msg("issue getting file schema from file_service")
		return nil, fmt.Errorf("issue getting file schema from file_service")
	}

	return metadataRes.SourceMetadata.Schema, nil
}
