package compute

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

type QueryContext struct {
	ctx   context.Context
	owner *ent.Owner

	changedSinceTime *timestamppb.Timestamp
	compileResp      *v1alpha.CompileResponse
	dataToken        *ent.DataToken
	finalResultTime  *timestamppb.Timestamp
	limits           *v1alpha.ExecuteRequest_Limits
	destination      *v1alpha.Destination

	// is the dataToken the "current" one?
	isCurrentDataToken bool

	// map of `table-id`` to `*internal.SliceTable`
	sliceTableMap map[uuid.UUID]*internal.SliceTable
}

func GetNewQueryContext(ctx context.Context, owner *ent.Owner, changedSinceTime *timestamppb.Timestamp, compileResp *v1alpha.CompileResponse, dataToken *ent.DataToken, finalResultTime *timestamppb.Timestamp, isCurrentDataToken bool, limits *v1alpha.ExecuteRequest_Limits, destination *v1alpha.Destination, sliceRequest *v1alpha.SliceRequest, sliceTableMap map[uuid.UUID]*internal.SliceTable) (*QueryContext, context.CancelFunc) {
	queryContext, queryContextCancel := context.WithCancel(ctx)

	return &QueryContext{
		ctx:   queryContext,
		owner: owner,

		changedSinceTime: changedSinceTime,
		compileResp:      compileResp,
		dataToken:        dataToken,
		finalResultTime:  finalResultTime,
		limits:           limits,
		destination:      destination,
		sliceTableMap:    sliceTableMap,

		isCurrentDataToken: isCurrentDataToken,
	}, queryContextCancel
}

func (qc *QueryContext) GetTableIDs() []uuid.UUID {
	tableIDs := make([]uuid.UUID, len(qc.sliceTableMap))
	i := 0
	for tableID := range qc.sliceTableMap {
		tableIDs[i] = tableID
		i += 1
	}
	return tableIDs
}

func (qc *QueryContext) GetSlices() []*internal.SliceInfo {
	slices := []*internal.SliceInfo{}
	for _, sliceTable := range qc.sliceTableMap {
		slices = append(slices, sliceTable.GetSlices()...)
	}
	return slices
}

func (qc *QueryContext) GetComputeTables() []*v1alpha.ComputeTable {
	computeTables := make([]*v1alpha.ComputeTable, len(qc.sliceTableMap))
	i := 0

	for _, sliceTable := range qc.sliceTableMap {
		computeTables[i] = convertKaskadaTableToComputeTable(sliceTable.KaskadaTable)

		for _, fileSet := range sliceTable.FileSetMap {

			computeFileset := &v1alpha.ComputeTable_FileSet{
				SlicePlan:     fileSet.SliceInfo.Plan,
				PreparedFiles: getComputePreparedFiles(fileSet.PrepareJobs),
			}

			computeTables[i].FileSets = append(computeTables[i].FileSets, computeFileset)
		}
		i++
	}
	return computeTables
}

func (qc *QueryContext) Cancelled() bool {
	return qc.ctx.Err() != nil
}
