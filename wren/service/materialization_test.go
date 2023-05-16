package service

import (
	"context"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/ent"

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

		Context("the compiled query includes both pulsar and object store sources", func() {
			It("should throw error", func() {
				mockComputeManager := compute.NewMockComputeManager(GinkgoT())
				expectedCompileResponse := v1alpha.CompileResponse{
					MissingNames:    []string{},
					FenlDiagnostics: nil,
					Plan:            &v1alpha.ComputePlan{},
					FreeNames:       []string{"file_backed_source", "pulsar_backed_source"},
				}
				mockComputeManager.EXPECT().CompileQuery(mock.Anything, owner, "nachos", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&expectedCompileResponse, nil)

			})
		})

		It("works without error", func() {
			mockComputeManager := compute.NewMockComputeManager(GinkgoT())
			expectedCompileResponse := v1alpha.CompileResponse{}

			mockComputeManager.EXPECT().CompileQuery(mock.Anything, owner, "nachos", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&expectedCompileResponse, nil)

			materializationService := &materializationService{
				UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
				computeManager: mockComputeManager,
			}
			response, err := materializationService.createMaterialization(context.Background(), owner, &v1alpha.CreateMaterializationRequest{
				Materialization: &v1alpha.Materialization{
					Expression:  "nachos",
					Destination: pulsarDestination,
				},
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(response).ShouldNot(BeNil())
			Expect(response.Analysis).ShouldNot(BeNil())
			Expect(response.Analysis.CanExecute).Should(BeTrue())
			Expect(response.Materialization).ShouldNot(BeNil())

		})
	})

})
