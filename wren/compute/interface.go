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
	CompileQuery(ctx context.Context, owner *ent.Owner, query string, requestViews []*v1alpha.WithView, isFormula bool, isExperimental bool, sliceRequest *v1alpha.SliceRequest, resultBehavior v1alpha.Query_ResultBehavior) (*v1alpha.CompileResponse, error)
	GetFormulas(ctx context.Context, owner *ent.Owner, views *v2alpha.QueryViews) ([]*v1alpha.Formula, error)
	GetUsedViews(formulas []*v1alpha.Formula, compileResponse *v1alpha.CompileResponse) *v2alpha.QueryViews
	CompileQueryV2(ctx context.Context, owner *ent.Owner, expression string, formulas []*v1alpha.Formula, config *v2alpha.QueryConfig) (*v1alpha.CompileResponse, error)
	CreateCompileRequest(ctx context.Context, owner *ent.Owner, request *QueryRequest, options *QueryOptions) (*v1alpha.CompileRequest, error)
	RunCompileRequest(ctx context.Context, owner *ent.Owner, compileRequest *v1alpha.CompileRequest) (*CompileQueryResponse, error)
	GetOutputURI(owner *ent.Owner, planHash []byte) string
	InitiateQuery(queryContext *QueryContext) (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error)
	SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ComputeSnapshot)
	RunMaterializations(requestCtx context.Context, owner *ent.Owner)
	GetTablesForCompute(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, slicePlans []*v1alpha.SlicePlan) (map[uuid.UUID]*internal.SliceTable, error)
	GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error)
}
