package cmd

import (
	"reflect"
	"testing"

	"github.com/kaskada-ai/kaskada/clients/cli/api"
	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func Test_exportMaterializations(t *testing.T) {
	type args struct {
		apiClient api.ApiClient
	}
	tests := []struct {
		name    string
		args    args
		want    []*apiv1alpha.Materialization
		wantErr bool
	}{
		{
			name: "should export materializations",
			args: args{
				apiClient: &mockApiClient{},
			},
			want: []*apiv1alpha.Materialization{
				{
					MaterializationName: "materialization",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := exportMaterializations(tt.args.apiClient)
			if (err != nil) != tt.wantErr {
				t.Errorf("exportMaterializations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("exportMaterializations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_exportTables(t *testing.T) {
	type args struct {
		apiClient api.ApiClient
	}
	tests := []struct {
		name    string
		args    args
		want    []*apiv1alpha.Table
		wantErr bool
	}{
		{
			name: "should export tables",
			args: args{
				apiClient: &mockApiClient{},
			},
			want: []*apiv1alpha.Table{
				{
					TableName: "table",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := exportTables(tt.args.apiClient)
			if (err != nil) != tt.wantErr {
				t.Errorf("exportMaterializations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("exportMaterializations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_exportViews(t *testing.T) {
	type args struct {
		apiClient api.ApiClient
	}
	tests := []struct {
		name    string
		args    args
		want    []*apiv1alpha.View
		wantErr bool
	}{
		{
			name: "should export views",
			args: args{
				apiClient: &mockApiClient{},
			},
			want: []*apiv1alpha.View{
				{
					ViewName: "view",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := exportViews(tt.args.apiClient)
			if (err != nil) != tt.wantErr {
				t.Errorf("exportMaterializations() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("exportMaterializations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_exportMaterialization(t *testing.T) {
	type args struct {
		apiClient           api.ApiClient
		materializationName string
	}
	tests := []struct {
		name    string
		args    args
		want    *apiv1alpha.Materialization
		wantErr bool
	}{
		{
			name: "should call get materialization on export materialization",
			args: args{
				apiClient:           &mockApiClient{},
				materializationName: "materialization",
			},
			want: &apiv1alpha.Materialization{
				MaterializationName: "materialization",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := exportMaterialization(tt.args.apiClient, tt.args.materializationName)
			if (err != nil) != tt.wantErr {
				t.Errorf("exportMaterialization() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("exportMaterialization() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_exportTable(t *testing.T) {
	type args struct {
		apiClient api.ApiClient
		tableName string
	}
	tests := []struct {
		name    string
		args    args
		want    *apiv1alpha.Table
		wantErr bool
	}{
		{
			name: "should call get table on export table",
			args: args{
				apiClient: &mockApiClient{},
				tableName: "table",
			},
			want: &apiv1alpha.Table{
				TableName: "table",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := exportTable(tt.args.apiClient, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("exportTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("exportTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_exportView(t *testing.T) {
	type args struct {
		apiClient api.ApiClient
		viewName  string
	}
	tests := []struct {
		name    string
		args    args
		want    *apiv1alpha.View
		wantErr bool
	}{
		{
			name: "should call get view for export view",
			args: args{
				apiClient: &mockApiClient{},
				viewName:  "view",
			},
			want: &apiv1alpha.View{
				ViewName: "view",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := exportView(tt.args.apiClient, tt.args.viewName)
			if (err != nil) != tt.wantErr {
				t.Errorf("exportView() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("exportView() = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockApiClient struct{}

// Create implements api.ApiClient
func (*mockApiClient) Create(item protoreflect.ProtoMessage) error {
	panic("unimplemented")
}

// Delete implements api.ApiClient
func (*mockApiClient) Delete(item protoreflect.ProtoMessage) error {
	panic("unimplemented")
}

// Get implements api.ApiClient
func (*mockApiClient) Get(item protoreflect.ProtoMessage) (protoreflect.ProtoMessage, error) {
	switch item.(type) {
	case *apiv1alpha.Materialization:
		return &apiv1alpha.Materialization{
			MaterializationName: "materialization",
		}, nil
	case *apiv1alpha.Table:
		return &apiv1alpha.Table{
			TableName: "table",
		}, nil
	case *apiv1alpha.View:
		return &apiv1alpha.View{
			ViewName: "view",
		}, nil
	default:
		return nil, nil
	}
}

// List implements api.ApiClient
func (*mockApiClient) List(item protoreflect.ProtoMessage) ([]protoreflect.ProtoMessage, error) {
	switch item.(type) {
	case *apiv1alpha.ListMaterializationsRequest:
		return []protoreflect.ProtoMessage{
			&apiv1alpha.Materialization{
				MaterializationName: "materialization",
			},
		}, nil
	case *apiv1alpha.ListTablesRequest:
		return []protoreflect.ProtoMessage{
			&apiv1alpha.Table{
				TableName: "table",
			},
		}, nil
	case *apiv1alpha.ListViewsRequest:
		return []protoreflect.ProtoMessage{
			&apiv1alpha.View{
				ViewName: "view",
			},
		}, nil
	default:
		return nil, nil
	}
}

// Query implements api.ApiClient
func (*mockApiClient) Query(*apiv1alpha.CreateQueryRequest) (*apiv1alpha.CreateQueryResponse, error) {
	panic("unimplemented")
}
