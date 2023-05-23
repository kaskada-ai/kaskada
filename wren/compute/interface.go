package compute

import (
	"context"

	"github.com/google/uuid"
	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
	"github.com/kaskada-ai/kaskada/wren/client"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

type ComputeManager interface {

	// compile related
	CompileQuery(ctx context.Context, owner *ent.Owner, query string, requestViews []*v1alpha.WithView, isFormula bool, isExperimental bool, sliceRequest *v1alpha.SliceRequest, resultBehavior v1alpha.Query_ResultBehavior) (*v1alpha.CompileResponse, error)
	CompileQueryV2(ctx context.Context, owner *ent.Owner, expression string, formulas []*v1alpha.Formula, config *v2alpha.QueryConfig) (*v1alpha.CompileResponse, error)
	CreateCompileRequest(ctx context.Context, owner *ent.Owner, request *QueryRequest, options *QueryOptions) (*v1alpha.CompileRequest, error)
	RunCompileRequest(ctx context.Context, owner *ent.Owner, compileRequest *v1alpha.CompileRequest) (*CompileQueryResponse, error)

	// execute related
	GetFormulas(ctx context.Context, owner *ent.Owner, views *v2alpha.QueryViews) ([]*v1alpha.Formula, error)
	GetOutputURI(owner *ent.Owner, planHash []byte) string
	GetTablesForCompute(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, slicePlans []*v1alpha.SlicePlan) (map[uuid.UUID]*internal.SliceTable, error)
	GetUsedViews(formulas []*v1alpha.Formula, compileResponse *v1alpha.CompileResponse) *v2alpha.QueryViews
	InitiateQuery(queryContext *QueryContext) (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error)
	SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ComputeSnapshot)

	// materialization related
	GetMaterializationStatus(ctx context.Context, materializationID string) (*v1alpha.ProgressInformation, error)
	ReconcileMaterialzations(ctx context.Context) error
	RunMaterializations(ctx context.Context, owner *ent.Owner)
	StartMaterialization(ctx context.Context, owner *ent.Owner, materializationID string, compileResp *v1alpha.CompileResponse, destination *v1alpha.Destination) error
	StopMaterialization(ctx context.Context, materializationID string) error

	// metadata related
	GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error)
}
