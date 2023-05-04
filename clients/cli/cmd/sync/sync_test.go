package sync

import (
	"errors"
	"testing"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func Test_compareResource(t *testing.T) {
	type args struct {
		apiClient api.ApiClient
		desired   protoreflect.ProtoMessage
	}
	tests := []struct {
		name string
		args args
		want CompareResult
	}{
		{
			name: "should return ERROR if unable to apiClient.Get resource",
			args: args{
				apiClient: &errorGetApiClient{},
				desired:   &apiv1alpha.View{},
			},
			want: Error,
		},
		{
			name: "should return CREATE if apiClient.Get returned not found",
			args: args{
				apiClient: &notFoundGetApiClient{},
				desired:   &apiv1alpha.View{},
			},
			want: Create,
		},
		{
			name: "should return REPLACE if apiClient.Get returned a different object",
			args: args{
				apiClient: &getApiClient{},
				desired: &apiv1alpha.View{
					ViewName: "non-existent-view",
				},
			},
			want: Replace,
		},
		{
			name: "should return SKIP if apiClient.Get returned the identical version",
			args: args{
				apiClient: &getApiClient{},
				desired: &apiv1alpha.View{
					ViewName: "view",
				},
			},
			want: Skip,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareResource(tt.args.apiClient, tt.args.desired); got != tt.want {
				t.Errorf("compareResource() = %v, want %v", got, tt.want)
			}
		})
	}
}

type errorGetApiClient struct{}

// LoadFile implements api.ApiClient
func (*errorGetApiClient) LoadFile(name string, fileInput *apiv1alpha.FileInput) error {
	panic("unimplemented")
}

// Create implements api.ApiClient
func (*errorGetApiClient) Create(item protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	panic("unimplemented")
}

// Delete implements api.ApiClient
func (*errorGetApiClient) Delete(item protoreflect.ProtoMessage, force bool) error {
	panic("unimplemented")
}

// Get implements api.ApiClient
func (*errorGetApiClient) Get(item protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	return nil, errors.New("test error")
}

// List implements api.ApiClient
func (*errorGetApiClient) List(item protoreflect.ProtoMessage) ([]protoreflect.ProtoMessage, error) {
	panic("unimplemented")
}

// Query implements api.ApiClient
func (*errorGetApiClient) Query(*apiv1alpha.CreateQueryRequest) (*apiv1alpha.CreateQueryResponse, error) {
	panic("unimplemented")
}

type notFoundGetApiClient struct{}

// LoadFile implements api.ApiClient
func (*notFoundGetApiClient) LoadFile(name string, fileInput *apiv1alpha.FileInput) error {
	panic("unimplemented")
}

// Create implements api.ApiClient
func (*notFoundGetApiClient) Create(item protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	panic("unimplemented")
}

// Delete implements api.ApiClient
func (*notFoundGetApiClient) Delete(item protoreflect.ProtoMessage, force bool) error {
	panic("unimplemented")
}

// Get implements api.ApiClient
func (*notFoundGetApiClient) Get(item protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	return nil, status.Error(codes.NotFound, "test error")
}

// List implements api.ApiClient
func (*notFoundGetApiClient) List(item protoreflect.ProtoMessage) ([]protoreflect.ProtoMessage, error) {
	panic("unimplemented")
}

// Query implements api.ApiClient
func (*notFoundGetApiClient) Query(*apiv1alpha.CreateQueryRequest) (*apiv1alpha.CreateQueryResponse, error) {
	panic("unimplemented")
}

type getApiClient struct{}

func (*getApiClient) LoadFile(name string, fileInput *apiv1alpha.FileInput) error {
	panic("unimplemented")
}

// Create implements api.ApiClient
func (*getApiClient) Create(item protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	panic("unimplemented")
}

// Delete implements api.ApiClient
func (*getApiClient) Delete(item protoreflect.ProtoMessage, force bool) error {
	panic("unimplemented")
}

// Get implements api.ApiClient
func (*getApiClient) Get(item protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	return &apiv1alpha.View{
		ViewName: "view",
		ViewId:   "should_ignore_this_field",
	}, nil
}

// List implements api.ApiClient
func (*getApiClient) List(item protoreflect.ProtoMessage) ([]protoreflect.ProtoMessage, error) {
	panic("unimplemented")
}

// Query implements api.ApiClient
func (*getApiClient) Query(*apiv1alpha.CreateQueryRequest) (*apiv1alpha.CreateQueryResponse, error) {
	panic("unimplemented")
}
