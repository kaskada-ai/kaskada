package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

func ConvertURIForCompute(URI string) string {
	return strings.TrimPrefix(URI, "file://")
}

func ConvertURIForManager(URI string) string {
	if strings.HasPrefix(URI, "/") {
		return fmt.Sprintf("file://%s", URI)
	}
	return URI
}

func reMapSparrowError(ctx context.Context, err error) error {
	subLogger := log.Ctx(ctx).With().Str("method", "manager.reMapSparrowError").Logger()
	inStatus, ok := status.FromError(err)
	if !ok {
		subLogger.Error().Msg("unexpected: compute error did not include error-status")
		return err
	}
	outStatus := status.New(inStatus.Code(), inStatus.Message())

	for _, detail := range inStatus.Details() {
		switch t := detail.(type) {
		case protoiface.MessageV1:
			outStatus, err = outStatus.WithDetails(t)
			if err != nil {
				subLogger.Error().Err(err).Interface("detail", t).Msg("unable to add detail to re-mapped error details")
			}
		default:
			subLogger.Error().Err(err).Interface("detail", t).Msg("unexpected: detail from compute doesn't implement the protoifam.MessageV1 interface")
		}
	}
	return outStatus.Err()
}

func convertKaskadaTableToComputeTable(kaskadaTable *ent.KaskadaTable) *v1alpha.ComputeTable {
	if kaskadaTable == nil {
		return nil
	}
	computeTable := &v1alpha.ComputeTable{
		Config: &v1alpha.TableConfig{
			Name:            kaskadaTable.Name,
			Uuid:            kaskadaTable.ID.String(),
			TimeColumnName:  kaskadaTable.TimeColumnName,
			GroupColumnName: kaskadaTable.EntityKeyColumnName,
			Grouping:        kaskadaTable.GroupingID,
			Source:          kaskadaTable.Source,
		},
		Metadata: &v1alpha.TableMetadata{
			Schema: kaskadaTable.MergedSchema,
		},
		FileSets: []*v1alpha.ComputeTable_FileSet{},
	}

	if kaskadaTable.SubsortColumnName != nil {
		computeTable.Config.SubsortColumnName = &wrapperspb.StringValue{Value: *kaskadaTable.SubsortColumnName}
	}
	return computeTable
}

func getComputePreparedFiles(prepareJobs []*ent.PrepareJob) []*v1alpha.PreparedFile {
	computePreparedFiles := []*v1alpha.PreparedFile{}
	for _, prepareJob := range prepareJobs {
		for _, preparedFile := range prepareJob.Edges.PreparedFiles {
			metadataPath := ""
			if preparedFile.MetadataPath != nil {
				metadataPath = *preparedFile.MetadataPath
			}
			computePreparedFiles = append(computePreparedFiles, &v1alpha.PreparedFile{
				Path:         ConvertURIForCompute(preparedFile.Path),
				MaxEventTime: timestamppb.New(time.Unix(0, preparedFile.MaxEventTime)),
				MinEventTime: timestamppb.New(time.Unix(0, preparedFile.MinEventTime)),
				NumRows:      preparedFile.RowCount,
				MetadataPath: ConvertURIForCompute(metadataPath),
			})
		}
	}
	return computePreparedFiles
}
