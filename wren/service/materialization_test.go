package service

import (
	"context"

	"github.com/google/uuid"
	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/kaskada-ai/kaskada/wren/internal"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mock "github.com/stretchr/testify/mock"
)

var _ = Describe("MaterializationService", func() {
	var (
		owner                  *ent.Owner
		objectStoreDestination = &v1alpha.Destination{
			Destination: &v1alpha.Destination_ObjectStore{
				ObjectStore: &v1alpha.ObjectStoreDestination{
					FileType:        v1alpha.FileType_FILE_TYPE_CSV,
					OutputPrefixUri: "gs://some-bucket/some-prefix",
				},
			},
		}
		pulsarDestination = &v1alpha.Destination{
			Destination: &v1alpha.Destination_Pulsar{
				Pulsar: &v1alpha.PulsarDestination{
					Config: &v1alpha.PulsarConfig{},
				},
			},
		}
	)

	BeforeEach(func() {
		owner = &ent.Owner{}
	})

	Context("CreateMaterialization", func() {
		Context("missing materialization", func() {

			It("should throw error", func() {
				materializationService := &materializationService{
					UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
				}
				response, err := materializationService.createMaterialization(context.Background(), owner, &v1alpha.CreateMaterializationRequest{})
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(Equal("missing materialization definition"))
				Expect(response).Should(BeNil())
			})
		})

		Context("missing expression", func() {
			It("should throw error", func() {
				materializationService := &materializationService{
					UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
				}
				response, err := materializationService.createMaterialization(context.Background(), owner, &v1alpha.CreateMaterializationRequest{
					Materialization: &v1alpha.Materialization{
						Expression:  "",
						Destination: objectStoreDestination,
					},
				})
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(Equal("missing materialization expression"))
				Expect(response).Should(BeNil())
			})
		})

		Context("missing destination", func() {
			It("should throw error", func() {
				materializationService := &materializationService{
					UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
				}

				response, err := materializationService.createMaterialization(context.Background(), owner, &v1alpha.CreateMaterializationRequest{
					Materialization: &v1alpha.Materialization{
						Expression: "nachos",
					},
				})
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(Equal("missing materialization destination"))
				Expect(response).Should(BeNil())
			})
		})

		Context("the compiled materialization includes both pulsar and object store sources", func() {
			It("should throw error", func() {
				freeNames := []string{"file_backed_source", "pulsar_backed_source"}

				mockMaterializationManager := compute.NewMockMaterializationManager(GinkgoT())
				newMaterialization := &v1alpha.Materialization{
					Expression:  "nachos",
					Destination: pulsarDestination,
				}

				expectedCompileResponse := &v1alpha.CompileResponse{
					MissingNames:    []string{},
					FenlDiagnostics: nil,
					Plan:            &v1alpha.ComputePlan{},
					FreeNames:       freeNames,
				}
				mockMaterializationManager.EXPECT().CompileV1Materialization(mock.Anything, owner, newMaterialization).Return(expectedCompileResponse, nil, nil)

				mockKaskadaTableClient := internal.NewMockKaskadaTableClient(GinkgoT())
				tablesResponse := map[string]*ent.KaskadaTable{
					"file_backed_source": {
						Name:   "file_backed_source",
						Source: &v1alpha.Source{Source: &v1alpha.Source_Kaskada{Kaskada: &v1alpha.KaskadaSource{}}},
					},
					"pulsar_backed_source": {
						Name:   "pulsar_backed_source",
						Source: &v1alpha.Source{Source: &v1alpha.Source_Pulsar{Pulsar: &v1alpha.PulsarSource{}}},
					},
				}

				mockKaskadaTableClient.EXPECT().GetKaskadaTablesFromNames(mock.Anything, owner, freeNames).Return(tablesResponse, nil)

				materializationService := &materializationService{
					UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
					computeManager:         nil,
					materializationManager: mockMaterializationManager,
					kaskadaTableClient:     mockKaskadaTableClient,
				}

				response, err := materializationService.createMaterialization(context.Background(), owner, &v1alpha.CreateMaterializationRequest{
					Materialization: newMaterialization,
				})
				Expect(err).Should(HaveOccurred())
				Expect(err.Error()).Should(Equal("cannot materialize tables from different source types"))
				Expect(response).Should(BeNil())
			})
		})

		Context("the compiled query includes only object store sources", func() {
			It("creates a file-based materialization", func() {
				freeNames := []string{"file_backed_source1", "file_backed_source2"}
				expression := "file_backed_to_pulsar"
				materializationName := "NAME_" + expression

				newMaterialization := &v1alpha.Materialization{
					MaterializationName: materializationName,
					Expression:          expression,
					Destination:         pulsarDestination,
				}

				mockMaterializationManager := compute.NewMockMaterializationManager(GinkgoT())
				expectedCompileResponse := &v1alpha.CompileResponse{
					MissingNames:    []string{},
					FenlDiagnostics: nil,
					Plan:            &v1alpha.ComputePlan{},
					FreeNames:       freeNames,
				}
				mockMaterializationManager.EXPECT().CompileV1Materialization(mock.Anything, owner, newMaterialization).Return(expectedCompileResponse, nil, nil)

				mockComputeManager := compute.NewMockComputeManager(GinkgoT())
				mockComputeManager.EXPECT().RunMaterializations(mock.Anything, owner)

				mockKaskadaTableClient := internal.NewMockKaskadaTableClient(GinkgoT())
				tablesResponse := map[string]*ent.KaskadaTable{
					"file_backed_source1": {
						Name:   "file_backed_source1",
						Source: &v1alpha.Source{Source: &v1alpha.Source_Kaskada{Kaskada: &v1alpha.KaskadaSource{}}},
					},
					"file_backed_source2": {
						Name:   "file_backed_source2",
						Source: &v1alpha.Source{Source: &v1alpha.Source_Kaskada{Kaskada: &v1alpha.KaskadaSource{}}},
					},
				}

				mockKaskadaTableClient.EXPECT().GetKaskadaTablesFromNames(mock.Anything, owner, freeNames).Return(tablesResponse, nil)

				mockKaskadaViewClient := internal.NewMockKaskadaViewClient(GinkgoT())
				mockKaskadaViewClient.EXPECT().GetKaskadaViewsFromNames(mock.Anything, owner, freeNames).Return(map[string]*ent.KaskadaView{}, nil)

				mockMaterializationClient := internal.NewMockMaterializationClient(GinkgoT())
				newMaterializationResponse := &ent.Materialization{
					ID:            uuid.New(),
					Name:          materializationName,
					Expression:    expression,
					Version:       int64(0),
					WithViews:     &v1alpha.WithViews{Views: []*v1alpha.WithView{}},
					Destination:   pulsarDestination,
					SliceRequest:  &v1alpha.SliceRequest{},
					Analysis:      getAnalysisFromCompileResponse(expectedCompileResponse),
					DataVersionID: int64(0),
					SourceType:    materialization.SourceTypeFiles,
				}

				mockMaterializationClient.EXPECT().CreateMaterialization(mock.Anything, owner, mock.Anything, mock.Anything).Return(newMaterializationResponse, nil)
				mockMaterializationClient.EXPECT().GetMaterialization(mock.Anything, owner, newMaterializationResponse.ID).Return(newMaterializationResponse, nil)

				mockDataTokenClient := internal.NewMockDataTokenClient(GinkgoT())
				dataTokenFromVersionResponse := &ent.DataToken{ID: uuid.New()}
				mockDataTokenClient.EXPECT().GetDataTokenFromVersion(mock.Anything, owner, newMaterializationResponse.DataVersionID).Return(dataTokenFromVersionResponse, nil)

				materializationService := &materializationService{
					UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
					computeManager:         mockComputeManager,
					materializationManager: mockMaterializationManager,
					dataTokenClient:        mockDataTokenClient,
					materializationClient:  mockMaterializationClient,
					kaskadaTableClient:     mockKaskadaTableClient,
					kaskadaViewClient:      mockKaskadaViewClient,
				}

				response, err := materializationService.createMaterialization(context.Background(), owner, &v1alpha.CreateMaterializationRequest{
					Materialization: newMaterialization,
				})
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response).ShouldNot(BeNil())
				Expect(response.Materialization).ShouldNot(BeNil())
				Expect(response.Materialization.DataTokenId).Should(Equal(dataTokenFromVersionResponse.ID.String()))
				Expect(response.Analysis).ShouldNot(BeNil())
			})
		})

		Context("the compiled query includes only stream sources", func() {
			It("creates a stream-based materialization", func() {
				freeNames := []string{"pulsar_backed_source1", "pulsar_backed_source2"}
				expression := "puslar_backed_to_object_store"
				materializationName := "NAME_" + expression

				newMaterialization := &v1alpha.Materialization{
					MaterializationName: materializationName,
					Expression:          expression,
					Destination:         pulsarDestination,
				}

				mockMaterializationManager := compute.NewMockMaterializationManager(GinkgoT())
				expectedCompileResponse := &v1alpha.CompileResponse{
					MissingNames:    []string{},
					FenlDiagnostics: nil,
					Plan:            &v1alpha.ComputePlan{},
					FreeNames:       freeNames,
				}
				mockMaterializationManager.EXPECT().CompileV1Materialization(mock.Anything, owner, newMaterialization).Return(expectedCompileResponse, nil, nil)

				mockKaskadaTableClient := internal.NewMockKaskadaTableClient(GinkgoT())
				tablesResponse := map[string]*ent.KaskadaTable{
					"pulsar_backed_source1": {
						Name:   "pulsar_backed_source1",
						Source: &v1alpha.Source{Source: &v1alpha.Source_Pulsar{Pulsar: &v1alpha.PulsarSource{}}},
					},
					"pulsar_backed_source2": {
						Name:   "pulsar_backed_source2",
						Source: &v1alpha.Source{Source: &v1alpha.Source_Pulsar{Pulsar: &v1alpha.PulsarSource{}}},
					},
				}

				mockKaskadaTableClient.EXPECT().GetKaskadaTablesFromNames(mock.Anything, owner, freeNames).Return(tablesResponse, nil)

				mockKaskadaViewClient := internal.NewMockKaskadaViewClient(GinkgoT())
				mockKaskadaViewClient.EXPECT().GetKaskadaViewsFromNames(mock.Anything, owner, freeNames).Return(map[string]*ent.KaskadaView{}, nil)

				mockMaterializationClient := internal.NewMockMaterializationClient(GinkgoT())
				newMaterializationResponse := &ent.Materialization{
					ID:            uuid.New(),
					Name:          materializationName,
					Expression:    expression,
					Version:       int64(0),
					WithViews:     &v1alpha.WithViews{Views: []*v1alpha.WithView{}},
					Destination:   objectStoreDestination,
					SliceRequest:  &v1alpha.SliceRequest{},
					Analysis:      getAnalysisFromCompileResponse(expectedCompileResponse),
					DataVersionID: int64(0),
					SourceType:    materialization.SourceTypeStreams,
				}

				mockMaterializationClient.EXPECT().CreateMaterialization(mock.Anything, owner, mock.Anything, mock.Anything).Return(newMaterializationResponse, nil)
				mockMaterializationClient.EXPECT().GetMaterialization(mock.Anything, owner, newMaterializationResponse.ID).Return(newMaterializationResponse, nil)

				mockMaterializationManager.EXPECT().StartMaterialization(mock.Anything, owner, newMaterializationResponse.ID.String(), expectedCompileResponse, objectStoreDestination).Return(nil)

				mockDataTokenClient := internal.NewMockDataTokenClient(GinkgoT())
				dataTokenFromVersionResponse := &ent.DataToken{ID: uuid.New()}
				mockDataTokenClient.EXPECT().GetDataTokenFromVersion(mock.Anything, owner, newMaterializationResponse.DataVersionID).Return(dataTokenFromVersionResponse, nil)

				materializationService := &materializationService{
					UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
					computeManager:         nil,
					materializationManager: mockMaterializationManager,
					dataTokenClient:        mockDataTokenClient,
					materializationClient:  mockMaterializationClient,
					kaskadaTableClient:     mockKaskadaTableClient,
					kaskadaViewClient:      mockKaskadaViewClient,
				}

				response, err := materializationService.createMaterialization(context.Background(), owner, &v1alpha.CreateMaterializationRequest{
					Materialization: newMaterialization,
				})
				Expect(err).ShouldNot(HaveOccurred())
				Expect(response).ShouldNot(BeNil())
				Expect(response.Materialization).ShouldNot(BeNil())
				Expect(response.Materialization.DataTokenId).Should(Equal(dataTokenFromVersionResponse.ID.String()))
				Expect(response.Analysis).ShouldNot(BeNil())
			})
		})
	})
})
