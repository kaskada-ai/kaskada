package service

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	pb "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

func TestDependencyAnalyzer_Analyze(t *testing.T) {
	owner := &ent.Owner{}

	sampleTable := "sample_table"

	sampleView := &ent.KaskadaView{
		ID:         uuid.MustParse("cafeb0ba-0000-0000-c0de-0000000ff1ce"),
		Name:       "sample_view",
		Expression: "viewExpression",
		Analysis: &pb.Analysis{
			FreeNames: []string{sampleTable},
		},
	}

	sampleMaterialization := &ent.Materialization{
		ID:   uuid.MustParse("facef00d-0000-0000-c0de-0000000ff1ce"),
		Name: "sample_materialization",
		Analysis: &pb.Analysis{
			FreeNames: []string{sampleTable},
		},
	}

	getErrorViewClient := func() internal.KaskadaViewClient {
		mockViewClient := internal.NewMockKaskadaViewClient(t)
		mockViewClient.EXPECT().GetKaskadaViewsWithDependency(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some listing view error"))
		return mockViewClient
	}

	getEmptyViewClient := func() internal.KaskadaViewClient {
		mockViewClient := internal.NewMockKaskadaViewClient(t)
		mockViewClient.EXPECT().GetKaskadaViewsWithDependency(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*ent.KaskadaView{}, nil)
		return mockViewClient
	}

	getSingleViewClient := func() internal.KaskadaViewClient {
		mockViewClient := internal.NewMockKaskadaViewClient(t)
		mockViewClient.EXPECT().GetKaskadaViewsWithDependency(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*ent.KaskadaView{sampleView}, nil)
		return mockViewClient

	}

	getErrorMaterializationClient := func() internal.MaterializationClient {
		mockMaterializationClient := internal.NewMockMaterializationClient(t)
		mockMaterializationClient.EXPECT().GetMaterializationsWithDependency(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some listing materialization error"))
		return mockMaterializationClient
	}

	getEmptyMaterializationClient := func() internal.MaterializationClient {
		mockMaterializationClient := internal.NewMockMaterializationClient(t)
		mockMaterializationClient.EXPECT().GetMaterializationsWithDependency(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*ent.Materialization{}, nil)
		return mockMaterializationClient
	}

	getSingleMaterializationClient := func() internal.MaterializationClient {
		mockMaterializationClient := internal.NewMockMaterializationClient(t)
		mockMaterializationClient.EXPECT().GetMaterializationsWithDependency(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*ent.Materialization{sampleMaterialization}, nil)
		return mockMaterializationClient
	}

	type fields struct {
		view            internal.KaskadaViewClient
		materialization internal.MaterializationClient
	}
	type args struct {
		name           string
		dependencyType schema.DependencyType
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ResourceDependency
		wantErr bool
		err     error
	}{
		{
			name: "should return error if unable to list views",
			fields: fields{
				view:            getErrorViewClient(),
				materialization: internal.NewMockMaterializationClient(t),
			},
			args:    args{name: "", dependencyType: schema.DependencyType_Unknown},
			want:    nil,
			wantErr: true,
			err:     errors.New("some listing view error"),
		},
		{
			name: "should return error if unable to list materializations",
			fields: fields{
				view:            getEmptyViewClient(),
				materialization: getErrorMaterializationClient(),
			},
			args:    args{name: "", dependencyType: schema.DependencyType_Unknown},
			want:    nil,
			wantErr: true,
			err:     errors.New("some listing materialization error"),
		},
		{
			name: "should return view if table is a dependency",
			fields: fields{
				view:            getSingleViewClient(),
				materialization: getEmptyMaterializationClient(),
			},
			args: args{
				name:           sampleTable,
				dependencyType: schema.DependencyType_Table,
			},
			want: &ResourceDependency{
				resourceName:     sampleTable,
				resourceType:     "table",
				views:            []*ent.KaskadaView{sampleView},
				materializations: []*ent.Materialization{},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "should return materialization if table is a dependency",
			fields: fields{
				view:            getEmptyViewClient(),
				materialization: getSingleMaterializationClient(),
			},
			args: args{
				name:           sampleTable,
				dependencyType: schema.DependencyType_Table,
			},
			want: &ResourceDependency{
				resourceName:     sampleTable,
				resourceType:     "table",
				views:            []*ent.KaskadaView{},
				materializations: []*ent.Materialization{sampleMaterialization},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "should return view and materialization if table is a dependency",
			fields: fields{
				view:            getSingleViewClient(),
				materialization: getSingleMaterializationClient(),
			},
			args: args{
				name:           sampleTable,
				dependencyType: schema.DependencyType_Table,
			},
			want: &ResourceDependency{
				resourceName:     sampleTable,
				resourceType:     "table",
				views:            []*ent.KaskadaView{sampleView},
				materializations: []*ent.Materialization{sampleMaterialization},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DependencyAnalyzer{
				kaskadaViewClient:     tt.fields.view,
				materializationClient: tt.fields.materialization,
			}
			ctx := context.Background()
			got, err := d.Analyze(ctx, owner, tt.args.name, tt.args.dependencyType)
			if err != nil {
				if tt.wantErr {
					if err.Error() != tt.err.Error() {
						t.Errorf("DependencyAnalyzer.Analyze() error = %v, want %v", err, tt.err)
					}
				} else {
					t.Errorf("DependencyAnalyzer.Analyze() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DependencyAnalyzer.Analyze() = %v, want %v", got, tt.want)
			}
		})
	}
}
