package utils

import (
	"reflect"
	"testing"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestProtoToYaml(t *testing.T) {
	testViewYaml := []byte(`viewName: test-view
expression: |-
  last_feature
  another_feature
`)

	testView := &apiv1alpha.View{
		ViewName: "test-view",
		Expression: `last_feature
another_feature`,
	}

	testTableYaml := []byte(`tableName: test-table
timeColumnName: time
entityKeyColumnName: entity
groupingId: grouping
`)

	testTable := &apiv1alpha.Table{
		TableName:           "test-table",
		TimeColumnName:      "time",
		EntityKeyColumnName: "entity",
		GroupingId:          "grouping",
	}

	type args struct {
		proto protoreflect.ProtoMessage
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "should marshal a view",
			args:    args{proto: testView},
			want:    testViewYaml,
			wantErr: false,
		},
		{
			name:    "should marshal a table",
			args:    args{proto: testTable},
			want:    testTableYaml,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProtoToYaml(tt.args.proto)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProtoToYaml() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				print(string(got))
				print(string(tt.want))
				t.Errorf("ProtoToYaml() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestYamlToSpec(t *testing.T) {
	testYaml := []byte(`
tables:
- table_name: test_table
  time_column_name: time_column
  entity_key_column_name: entity_key
- table_name: test_table2
  time_column_name: time_column2
  entity_key_column_name: entity_key2
views:
- view_name: test_view
  expression: |-
    {
      name: test_table.name,
      path: test_table2.entity_key2
    }
`)

	type args struct {
		yamlData []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *apiv1alpha.Spec
		wantErr bool
	}{
		{
			name: "should unmarshal a yaml byte slice to a spec",
			args: args{
				yamlData: testYaml,
			},
			want: &apiv1alpha.Spec{
				Tables: []*apiv1alpha.Table{
					{
						TableName:           "test_table",
						TimeColumnName:      "time_column",
						EntityKeyColumnName: "entity_key",
					},
					{
						TableName:           "test_table2",
						TimeColumnName:      "time_column2",
						EntityKeyColumnName: "entity_key2",
					},
				},
				Views: []*apiv1alpha.View{
					{
						ViewName: "test_view",
						Expression: `{
  name: test_table.name,
  path: test_table2.entity_key2
}`,
					},
				},
				Materializations: []*apiv1alpha.Materialization{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := YamlToSpec(tt.args.yamlData)
			if (err != nil) != tt.wantErr {
				t.Errorf("YamlToSpec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if string(got.String()) != string(tt.want.String()) {
				t.Errorf("YamlToSpec() = %v, want %v", got, tt.want)
			}
		})
	}
}
