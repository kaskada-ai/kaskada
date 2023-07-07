package compute

import (
	"context"

	"github.com/google/uuid"
	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
	ent_materialization "github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/kaskada-ai/kaskada/wren/internal"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CompileManager", func() {

	var (
		ctx   context.Context
		owner *ent.Owner

		defaultUUID = uuid.MustParse("00000000-0000-0000-0000-000000000000")

		mockComputeServiceClient     *v1alpha.MockComputeServiceClient
		mockFileServiceClient        *v1alpha.MockFileServiceClient
		mockPreparationServiceClient *v1alpha.MockPreparationServiceClient
		mockKaskadaTableClient       *internal.MockKaskadaTableClient
		mockKaskadaViewClient        *internal.MockKaskadaViewClient

		objectStoreDestination = &v1alpha.Destination{
			Destination: &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType:        v1alpha.FileType_FILE_TYPE_CSV,
					OutputPrefixUri: "gs://some-bucket/some-prefix",
				},
			},
		}

		sliceRequest = &v1alpha.SliceRequest{
			Slice: &v1alpha.SliceRequest_Percent{
				Percent: &v1alpha.SliceRequest_PercentSlice{
					Percent: 42,
				},
			},
		}

		persistedViews = []*ent.KaskadaView{
			{
				Name:       "persisted_view",
				Expression: "persisted_view_expression",
			},
			{
				Name:       "overwritten_view",
				Expression: "overwritten_view_expression",
			},
		}

		persistedTables = []*ent.KaskadaTable{
			{
				ID:           defaultUUID,
				Name:         "persisted_table1",
				MergedSchema: &v1alpha.Schema{},
			},
			{
				ID:           defaultUUID,
				Name:         "persisted_table2",
				MergedSchema: &v1alpha.Schema{},
			},
		}
	)

	BeforeEach(func() {
		ctx = context.Background()
		owner = &ent.Owner{}

		mockKaskadaTableClient = internal.NewMockKaskadaTableClient(GinkgoT())
		mockKaskadaViewClient = internal.NewMockKaskadaViewClient(GinkgoT())
		mockComputeServiceClient = v1alpha.NewMockComputeServiceClient(GinkgoT())
		mockFileServiceClient = v1alpha.NewMockFileServiceClient(GinkgoT())
		mockPreparationServiceClient = v1alpha.NewMockPreparationServiceClient(GinkgoT())

	})

	Context("CompileEntMaterialization", func() {
		It("should compile a materialization", func() {
			mockKaskadaViewClient.EXPECT().GetAllKaskadaViews(ctx, owner).Return(persistedViews, nil)

			mockKaskadaTableClient.EXPECT().GetAllKaskadaTables(ctx, owner).Return(persistedTables, nil)

			entMaterialization := &ent.Materialization{
				Name:         "ent_materialization",
				Expression:   "ent_materialization_expression",
				Destination:  objectStoreDestination,
				SliceRequest: sliceRequest,
				WithViews: &v1alpha.WithViews{
					Views: []*v1alpha.WithView{
						{
							Name:       "with_view",
							Expression: "with_view_expression",
						},
						{
							Name:       "overwritten_view",
							Expression: "overwritten_view_expression2",
						},
					},
				},
				SourceType: ent_materialization.SourceTypeFiles,
			}

			computeTables := []*v1alpha.ComputeTable{
				{
					Config: &v1alpha.TableConfig{
						Name: "persisted_table1",
						Uuid: defaultUUID.String(),
					},
					Metadata: &v1alpha.TableMetadata{
						Schema: &v1alpha.Schema{},
					},
					FileSets: []*v1alpha.ComputeTable_FileSet{},
				},
				{
					Config: &v1alpha.TableConfig{
						Name: "persisted_table2",
						Uuid: defaultUUID.String(),
					},
					Metadata: &v1alpha.TableMetadata{
						Schema: &v1alpha.Schema{},
					},
					FileSets: []*v1alpha.ComputeTable_FileSet{},
				},
			}

			formulas := []*v1alpha.Formula{
				{
					Name:           "persisted_view",
					Formula:        "persisted_view_expression",
					SourceLocation: "Persisted View: persisted_view",
				},
				{
					Name:           "overwritten_view",
					Formula:        "overwritten_view_expression2",
					SourceLocation: "Requested View: overwritten_view",
				},
				{
					Name:           "with_view",
					Formula:        "with_view_expression",
					SourceLocation: "Requested View: with_view",
				},
			}

			matchingFunc := func(compileRequest *v1alpha.CompileRequest) bool {
				expectedCompileRequest := &v1alpha.CompileRequest{
					Experimental: false,
					FeatureSet: &v1alpha.FeatureSet{
						Formulas: formulas,
						Query:    entMaterialization.Expression,
					},
					PerEntityBehavior: v1alpha.PerEntityBehavior_PER_ENTITY_BEHAVIOR_FINAL,
					SliceRequest:      sliceRequest,
					Tables:            computeTables,
					ExpressionKind:    v1alpha.CompileRequest_EXPRESSION_KIND_COMPLETE,
				}

				return proto.Equal(expectedCompileRequest, compileRequest)
			}

			compileResponse := &v1alpha.CompileResponse{
				FreeNames: []string{"with_view", "overwritten_view", "persisted_table1"},
			}

			mockComputeServiceClient.On("Compile", mock.MatchedBy(matchingFunc)).Return(compileResponse,nil)

			computeClients := newMockComputeServiceClients(mockFileServiceClient, mockPreparationServiceClient, mockComputeServiceClient)
			compManager := &compileManager{
				computeClients:     computeClients,
				kaskadaTableClient: mockKaskadaTableClient,
				kaskadaViewClient:  mockKaskadaViewClient,
			}

			compileResponse, views, err := compManager.CompileEntMaterialization(ctx, owner, entMaterialization)
			Expect(err).ToNot(HaveOccurred())
			Expect(compileResponse).ToNot(BeNil())
			Expect(views).ToNot(BeNil())

			expectedViews := []*v1alpha.View{
				{
					ViewName:   "with_view",
					Expression: "with_view_expression",
				},
				{
					ViewName:   "overwritten_view",
					Expression: "overwritten_view_expression2",
				},
			}

			Expect(views).To(Equal(expectedViews))
		})
	})
})
