package api

import (
	"reflect"
	"testing"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestGetName(t *testing.T) {
	type args struct {
		item protoreflect.ProtoMessage
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should get name for table",
			args: args{
				&apiv1alpha.Table{
					TableName: "table_name",
				},
			},
			want: "table_name",
		},
		{
			name: "should get name for view",
			args: args{
				&apiv1alpha.View{
					ViewName: "view_name",
				},
			},
			want: "view_name",
		},
		{
			name: "should get name for materialization",
			args: args{
				&apiv1alpha.Materialization{
					MaterializationName: "materialization_name",
				},
			},
			want: "materialization_name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetName(tt.args.item); got != tt.want {
				t.Errorf("GetName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_apiClient_Query(t *testing.T) {
	type fields struct {
		query           QueryClient
		materialization MaterializationClient
		table           TableClient
		view            ViewClient
	}
	type args struct {
		req *apiv1alpha.CreateQueryRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *apiv1alpha.CreateQueryResponse
		wantErr bool
	}{
		{
			name: "should send query to query client and return response",
			fields: fields{
				query: &mockQueryClient{},
			},
			args: args{req: &apiv1alpha.CreateQueryRequest{
				Query: &apiv1alpha.Query{
					Expression: "unit_test_query",
				},
			}},
			want:    &apiv1alpha.CreateQueryResponse{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := apiClient{
				query:           tt.fields.query,
				materialization: tt.fields.materialization,
				table:           tt.fields.table,
				view:            tt.fields.view,
			}
			got, err := c.Query(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("apiClient.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("apiClient.Query() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_apiClient_Create(t *testing.T) {
	type fields struct {
		query           QueryClient
		materialization MaterializationClient
		table           TableClient
		view            ViewClient
	}
	type args struct {
		item protoreflect.ProtoMessage
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "should call materialization client on create with materialization",
			fields: fields{materialization: &mockMaterializationClient{}},
			args: args{
				item: &apiv1alpha.Materialization{},
			},
			wantErr: false,
		},
		{
			name:   "should call table client on create with table",
			fields: fields{table: &mockTableClient{}},
			args: args{
				item: &apiv1alpha.Table{},
			},
			wantErr: false,
		},
		{
			name:   "should call view client on create with view",
			fields: fields{view: &mockViewClient{}},
			args: args{
				item: &apiv1alpha.View{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := apiClient{
				query:           tt.fields.query,
				materialization: tt.fields.materialization,
				table:           tt.fields.table,
				view:            tt.fields.view,
			}
			if err := c.Create(tt.args.item); (err != nil) != tt.wantErr {
				t.Errorf("apiClient.Create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_apiClient_Delete(t *testing.T) {
	type fields struct {
		query           QueryClient
		materialization MaterializationClient
		table           TableClient
		view            ViewClient
	}
	type args struct {
		item protoreflect.ProtoMessage
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "should call materialization client on delete with materialization",
			fields: fields{materialization: &mockMaterializationClient{}},
			args: args{
				item: &apiv1alpha.Materialization{},
			},
			wantErr: false,
		},
		{
			name:   "should call table client on delete with table",
			fields: fields{table: &mockTableClient{}},
			args: args{
				item: &apiv1alpha.Table{},
			},
			wantErr: false,
		},
		{
			name:   "should call view client on delete with view",
			fields: fields{view: &mockViewClient{}},
			args: args{
				item: &apiv1alpha.View{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := apiClient{
				query:           tt.fields.query,
				materialization: tt.fields.materialization,
				table:           tt.fields.table,
				view:            tt.fields.view,
			}
			if err := c.Delete(tt.args.item); (err != nil) != tt.wantErr {
				t.Errorf("apiClient.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_apiClient_Get(t *testing.T) {
	type fields struct {
		query           QueryClient
		materialization MaterializationClient
		table           TableClient
		view            ViewClient
	}
	type args struct {
		item protoreflect.ProtoMessage
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    protoreflect.ProtoMessage
		wantErr bool
	}{
		{
			name:   "should call materialization client on get with materialization",
			fields: fields{materialization: &mockMaterializationClient{}},
			args: args{
				item: &apiv1alpha.Materialization{
					MaterializationName: "materialization",
				},
			},
			want: &apiv1alpha.Materialization{
				MaterializationName: "materialization",
			},
			wantErr: false,
		},
		{
			name:   "should call table client on get with table",
			fields: fields{table: &mockTableClient{}},
			args: args{
				item: &apiv1alpha.Table{
					TableName: "table",
				},
			},
			want: &apiv1alpha.Table{
				TableName: "table",
			},
			wantErr: false,
		},
		{
			name:   "should call view client on get with view",
			fields: fields{view: &mockViewClient{}},
			args: args{
				item: &apiv1alpha.View{
					ViewName: "view",
				},
			},
			want: &apiv1alpha.View{
				ViewName: "view",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := apiClient{
				query:           tt.fields.query,
				materialization: tt.fields.materialization,
				table:           tt.fields.table,
				view:            tt.fields.view,
			}
			got, err := c.Get(tt.args.item)
			if (err != nil) != tt.wantErr {
				t.Errorf("apiClient.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("apiClient.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_apiClient_List(t *testing.T) {
	type fields struct {
		query           QueryClient
		materialization MaterializationClient
		table           TableClient
		view            ViewClient
	}
	type args struct {
		item protoreflect.ProtoMessage
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []protoreflect.ProtoMessage
		wantErr bool
	}{
		{
			name:   "should call materialization client on list with materialization",
			fields: fields{materialization: &mockMaterializationClient{}},
			args: args{
				item: &apiv1alpha.ListMaterializationsRequest{},
			},
			want: []protoreflect.ProtoMessage{
				&apiv1alpha.Materialization{},
			},
			wantErr: false,
		},
		{
			name:   "should call table client on list with table",
			fields: fields{table: &mockTableClient{}},
			args: args{
				item: &apiv1alpha.ListTablesRequest{},
			},
			want: []protoreflect.ProtoMessage{
				&apiv1alpha.Table{},
			},
			wantErr: false,
		},
		{
			name:   "should call view client on list with view",
			fields: fields{view: &mockViewClient{}},
			args: args{
				item: &apiv1alpha.ListViewsRequest{},
			},
			want: []protoreflect.ProtoMessage{
				&apiv1alpha.View{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := apiClient{
				query:           tt.fields.query,
				materialization: tt.fields.materialization,
				table:           tt.fields.table,
				view:            tt.fields.view,
			}
			got, err := c.List(tt.args.item)
			if (err != nil) != tt.wantErr {
				t.Errorf("apiClient.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("apiClient.List() = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockQueryClient struct{}

// Query implements QueryClient
func (*mockQueryClient) Query(request *apiv1alpha.CreateQueryRequest) (*apiv1alpha.CreateQueryResponse, error) {
	return &apiv1alpha.CreateQueryResponse{}, nil
}

type mockMaterializationClient struct{}

// Create implements MaterializationClient
func (*mockMaterializationClient) Create(item *apiv1alpha.Materialization) error {
	return nil
}

// Delete implements MaterializationClient
func (*mockMaterializationClient) Delete(name string) error {
	return nil
}

// Get implements MaterializationClient
func (*mockMaterializationClient) Get(name string) (*apiv1alpha.Materialization, error) {
	return &apiv1alpha.Materialization{
		MaterializationName: name,
	}, nil
}

// List implements MaterializationClient
func (*mockMaterializationClient) List() ([]*apiv1alpha.Materialization, error) {
	materializations := make([]*apiv1alpha.Materialization, 0, 1)
	materializations = append(materializations, &apiv1alpha.Materialization{})
	return materializations, nil
}

type mockTableClient struct{}

func (*mockTableClient) LoadFile(name string, fileInput *apiv1alpha.FileInput) error {
	return nil
}

// Create implements TableClient
func (*mockTableClient) Create(item *apiv1alpha.Table) error {
	return nil
}

// Delete implements TableClient
func (*mockTableClient) Delete(name string) error {
	return nil
}

// Get implements TableClient
func (*mockTableClient) Get(name string) (*apiv1alpha.Table, error) {
	return &apiv1alpha.Table{
		TableName: name,
	}, nil
}

// List implements TableClient
func (*mockTableClient) List() ([]*apiv1alpha.Table, error) {
	tables := make([]*apiv1alpha.Table, 0, 1)
	tables = append(tables, &apiv1alpha.Table{})
	return tables, nil
}

type mockViewClient struct{}

// Create implements ViewClient
func (*mockViewClient) Create(item *apiv1alpha.View) error {
	return nil
}

// Delete implements ViewClient
func (*mockViewClient) Delete(name string) error {
	return nil
}

// Get implements ViewClient
func (*mockViewClient) Get(name string) (*apiv1alpha.View, error) {
	return &apiv1alpha.View{
		ViewName: name,
	}, nil
}

// List implements ViewClient
func (*mockViewClient) List() ([]*apiv1alpha.View, error) {
	views := make([]*apiv1alpha.View, 0, 1)
	views = append(views, &apiv1alpha.View{})
	return views, nil
}
