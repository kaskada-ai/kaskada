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

type ComputeManagerMockConfig struct {
	CompileQueryResponse         func() (*v1alpha.CompileResponse, error)
	GetFormulasResponse          func() ([]*v1alpha.Formula, error)
	GetUsedViewsResponse         func() *v2alpha.QueryViews
	CompileQueryV2Response       func() (*v1alpha.CompileResponse, error)
	CreateCompileRequestResponse func() (*v1alpha.CompileRequest, error)
	RunCompileRequestResponse    func() (*CompileQueryResponse, error)
	GetOutputURIResponse         func() string
	InitiateQueryResponse        func() (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error)
	GetTablesForComputeResponse  func() (map[uuid.UUID]*internal.SliceTable, error)
	GetFileSchemaResponse        func() (*v1alpha.Schema, error)
}

// GetComputeManagerMockConfig returns a default ComputeManagerMockConfig with all methods returning nil
func GetComputeManagerMockConfig() ComputeManagerMockConfig {
	return ComputeManagerMockConfig{
		CompileQueryResponse: func() (*v1alpha.CompileResponse, error) {
			return nil, nil
		},
		GetFormulasResponse: func() ([]*v1alpha.Formula, error) {
			return nil, nil
		},
		GetUsedViewsResponse: func() *v2alpha.QueryViews {
			return nil
		},
		CompileQueryV2Response: func() (*v1alpha.CompileResponse, error) {
			return nil, nil
		},
		CreateCompileRequestResponse: func() (*v1alpha.CompileRequest, error) {
			return nil, nil
		},
		RunCompileRequestResponse: func() (*CompileQueryResponse, error) {
			return nil, nil
		},
		GetOutputURIResponse: func() string {
			return ""
		},
		InitiateQueryResponse: func() (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error) {
			return nil, nil, nil
		},
		GetTablesForComputeResponse: func() (map[uuid.UUID]*internal.SliceTable, error) {
			return nil, nil
		},
		GetFileSchemaResponse: func() (*v1alpha.Schema, error) {
			return nil, nil
		},
	}
}

type computeManagerMock struct {
	config ComputeManagerMockConfig
}

// NewComputeManagerMock creates a new ComputeManagerMock for testing purposes
func NewComputeManagerMock(config ComputeManagerMockConfig) ComputeManager {
	return &computeManagerMock{
		config: config,
	}
}

func (c *computeManagerMock) CompileQuery(ctx context.Context, owner *ent.Owner, query string, requestViews []*v1alpha.WithView, isFormula bool, isExperimental bool, sliceRequest *v1alpha.SliceRequest, resultBehavior v1alpha.Query_ResultBehavior) (*v1alpha.CompileResponse, error) {
	return c.config.CompileQueryResponse()
}

func (c *computeManagerMock) GetFormulas(ctx context.Context, owner *ent.Owner, views *v2alpha.QueryViews) ([]*v1alpha.Formula, error) {
	return c.config.GetFormulasResponse()
}

func (c *computeManagerMock) GetUsedViews(formulas []*v1alpha.Formula, compileResponse *v1alpha.CompileResponse) *v2alpha.QueryViews {
	return c.config.GetUsedViewsResponse()
}

func (c *computeManagerMock) CompileQueryV2(ctx context.Context, owner *ent.Owner, expression string, formulas []*v1alpha.Formula, config *v2alpha.QueryConfig) (*v1alpha.CompileResponse, error) {
	return c.config.CompileQueryV2Response()
}

func (c *computeManagerMock) CreateCompileRequest(ctx context.Context, owner *ent.Owner, request *QueryRequest, options *QueryOptions) (*v1alpha.CompileRequest, error) {
	return c.config.CreateCompileRequestResponse()
}

func (c *computeManagerMock) RunCompileRequest(ctx context.Context, owner *ent.Owner, compileRequest *v1alpha.CompileRequest) (*CompileQueryResponse, error) {
	return c.config.RunCompileRequestResponse()
}

func (c *computeManagerMock) GetOutputURI(owner *ent.Owner, planHash []byte) string {
	return c.config.GetOutputURIResponse()
}

func (c *computeManagerMock) InitiateQuery(queryContext *QueryContext) (client.ComputeServiceClient, v1alpha.ComputeService_ExecuteClient, error) {
	return c.config.InitiateQueryResponse()
}

func (c *computeManagerMock) SaveComputeSnapshots(queryContext *QueryContext, computeSnapshots []*v1alpha.ComputeSnapshot) {
}

func (c *computeManagerMock) RunMaterializations(requestCtx context.Context, owner *ent.Owner) {
}

func (c *computeManagerMock) GetTablesForCompute(ctx context.Context, owner *ent.Owner, dataToken *ent.DataToken, slicePlans []*v1alpha.SlicePlan) (map[uuid.UUID]*internal.SliceTable, error) {
	return c.config.GetTablesForComputeResponse()
}

func (c *computeManagerMock) GetFileSchema(ctx context.Context, fileInput internal.FileInput) (*v1alpha.Schema, error) {
	return c.config.GetFileSchemaResponse()
}
