package compute

import (
	"context"
	"testing"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"

	"github.com/kaskada-ai/kaskada/wren/client"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCompute(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Compute Suite")
}

type mockComputeServiceClients struct {
	fileServiceClient        v1alpha.MockFileServiceClient
	preparationServiceClient v1alpha.MockPreparationServiceClient
	computeServiceClient     v1alpha.MockComputeServiceClient
}

func newMockComputeServiceClients(fileServiceClient *v1alpha.MockFileServiceClient, preparationServiceClient *v1alpha.MockPreparationServiceClient, computeServiceClient *v1alpha.MockComputeServiceClient) client.ComputeClients {
	return &mockComputeServiceClients{
		fileServiceClient:        *fileServiceClient,
		preparationServiceClient: *preparationServiceClient,
		computeServiceClient:     *computeServiceClient,
	}
}

func (m *mockComputeServiceClients) NewFileServiceClient(ctx context.Context) client.FileServiceClient {
	return &mockFileServiceClient{
		MockFileServiceClient: m.fileServiceClient,
	}
}

func (m *mockComputeServiceClients) NewPrepareServiceClient(ctx context.Context) client.PrepareServiceClient {
	return &mockPrepareServiceClient{
		MockPreparationServiceClient: m.preparationServiceClient,
	}
}

func (m *mockComputeServiceClients) NewComputeServiceClient(ctx context.Context) client.ComputeServiceClient {
	return &mockComputeServiceClient{
		MockComputeServiceClient: m.computeServiceClient,
	}
}

type mockFileServiceClient struct {
	v1alpha.MockFileServiceClient
}

func (s mockFileServiceClient) Close() error {
	return nil
}

type mockPrepareServiceClient struct {
	v1alpha.MockPreparationServiceClient
}

func (s mockPrepareServiceClient) Close() error {
	return nil
}

type mockComputeServiceClient struct {
	v1alpha.MockComputeServiceClient
}

func (s mockComputeServiceClient) Close() error {
	return nil
}
