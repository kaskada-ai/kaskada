package service

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"

	pb "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/materialization"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

const (
	sampleTable           = "sample_table"
	sampleView            = "sample_view"
	sampleMaterialization = "sample_materialization"
	viewUUID              = "cafeb0ba-0000-0000-c0de-0000000ff1ce"
	materializationUUID   = "facef00d-0000-0000-c0de-0000000ff1ce"
)

func TestDependencyAnalyzer_Analyze(t *testing.T) {
	owner := &ent.Owner{}

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
				view:            &errorListViewsClient{},
				materialization: &emptyListMaterializationClient{},
			},
			args:    args{name: "", dependencyType: schema.DependencyType_Unknown},
			want:    nil,
			wantErr: true,
			err:     errors.New("some listing view error"),
		},
		{
			name: "should return error if unable to list materializations",
			fields: fields{
				view:            &emptyListViewsClient{},
				materialization: &errorListMaterializationClient{},
			},
			args:    args{name: "", dependencyType: schema.DependencyType_Unknown},
			want:    nil,
			wantErr: true,
			err:     errors.New("some list materializations error"),
		},
		{
			name: "should return view if table is a dependency",
			fields: fields{
				view:            &singleViewClient{},
				materialization: &emptyListMaterializationClient{},
			},
			args: args{
				name:           sampleTable,
				dependencyType: schema.DependencyType_Table,
			},
			want: &ResourceDependency{
				resourceName: sampleTable,
				resourceType: "table",
				views: []*ent.KaskadaView{
					{
						ID:         uuid.MustParse(viewUUID),
						Name:       sampleView,
						Expression: "viewExpression",
						Analysis: &pb.Analysis{
							FreeNames: []string{sampleTable},
						},
					},
				},
				materializations: []*ent.Materialization{},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "should return materialization if table is a dependency",
			fields: fields{
				view:            &emptyListViewsClient{},
				materialization: &singleListMaterializationClient{},
			},
			args: args{
				name:           sampleTable,
				dependencyType: schema.DependencyType_Table,
			},
			want: &ResourceDependency{
				resourceName: sampleTable,
				resourceType: "table",
				views:        []*ent.KaskadaView{},
				materializations: []*ent.Materialization{
					{
						ID:   uuid.MustParse(materializationUUID),
						Name: sampleMaterialization,
						Analysis: &pb.Analysis{
							FreeNames: []string{sampleTable},
						},
					},
				},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "should return view and materialization if table is a dependency",
			fields: fields{
				view:            &singleViewClient{},
				materialization: &singleListMaterializationClient{},
			},
			args: args{
				name:           sampleTable,
				dependencyType: schema.DependencyType_Table,
			},
			want: &ResourceDependency{
				resourceName: sampleTable,
				resourceType: "table",
				views: []*ent.KaskadaView{
					{
						ID:         uuid.MustParse(viewUUID),
						Name:       sampleView,
						Expression: "viewExpression",
						Analysis: &pb.Analysis{
							FreeNames: []string{sampleTable},
						},
					},
				},
				materializations: []*ent.Materialization{
					{
						ID:   uuid.MustParse(materializationUUID),
						Name: sampleMaterialization,
						Analysis: &pb.Analysis{
							FreeNames: []string{sampleTable},
						},
					},
				},
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

type errorListViewsClient struct{}

func (*errorListViewsClient) CreateKaskadaView(ctx context.Context, owner *ent.Owner, newView *ent.KaskadaView, dependencies []*ent.ViewDependency) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*errorListViewsClient) DeleteKaskadaView(ctx context.Context, owner *ent.Owner, view *ent.KaskadaView) error {
	panic("unimplemented")
}

func (*errorListViewsClient) GetAllKaskadaViews(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*errorListViewsClient) GetKaskadaView(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*errorListViewsClient) GetKaskadaViewByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*errorListViewsClient) GetKaskadaViewsFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*errorListViewsClient) GetKaskadaViewsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.KaskadaView, error) {
	return nil, errors.New("some listing view error")
}

func (*errorListViewsClient) ListKaskadaViews(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaView, error) {
	panic("unimplemented")
}

type emptyListViewsClient struct{}

func (*emptyListViewsClient) CreateKaskadaView(ctx context.Context, owner *ent.Owner, newView *ent.KaskadaView, dependencies []*ent.ViewDependency) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*emptyListViewsClient) DeleteKaskadaView(ctx context.Context, owner *ent.Owner, view *ent.KaskadaView) error {
	panic("unimplemented")
}

func (*emptyListViewsClient) GetAllKaskadaViews(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*emptyListViewsClient) GetKaskadaView(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*emptyListViewsClient) GetKaskadaViewByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*emptyListViewsClient) GetKaskadaViewsFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*emptyListViewsClient) GetKaskadaViewsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.KaskadaView, error) {
	return []*ent.KaskadaView{}, nil
}

func (*emptyListViewsClient) ListKaskadaViews(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaView, error) {
	panic("unimplemented")
}

type singleViewClient struct{}

func (*singleViewClient) CreateKaskadaView(ctx context.Context, owner *ent.Owner, newView *ent.KaskadaView, dependencies []*ent.ViewDependency) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*singleViewClient) DeleteKaskadaView(ctx context.Context, owner *ent.Owner, view *ent.KaskadaView) error {
	panic("unimplemented")
}

func (*singleViewClient) GetAllKaskadaViews(ctx context.Context, owner *ent.Owner) ([]*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*singleViewClient) GetKaskadaView(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*singleViewClient) GetKaskadaViewByName(ctx context.Context, owner *ent.Owner, name string) (*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*singleViewClient) GetKaskadaViewsFromNames(ctx context.Context, owner *ent.Owner, names []string) (map[string]*ent.KaskadaView, error) {
	panic("unimplemented")
}

func (*singleViewClient) GetKaskadaViewsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.KaskadaView, error) {
	return []*ent.KaskadaView{
		{
			ID:         uuid.MustParse(viewUUID),
			Name:       sampleView,
			Expression: "viewExpression",
			Analysis: &pb.Analysis{
				FreeNames: []string{sampleTable},
			},
		}}, nil
}

func (*singleViewClient) ListKaskadaViews(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.KaskadaView, error) {
	panic("unimplemented")
}

/*
type MaterializationClient interface {
	CreateMaterialization(ctx context.Context, owner *ent.Owner, newMaterialization *ent.Materialization, dependencies []*ent.MaterializationDependency) (*ent.Materialization, error)
	DeleteMaterialization(ctx context.Context, owner *ent.Owner, view *ent.Materialization) error
	GetAllMaterializations(ctx context.Context, owner *ent.Owner) ([]*ent.Materialization, error)
	GetMaterialization(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.Materialization, error)
	GetMaterializationByName(ctx context.Context, owner *ent.Owner, name string) (*ent.Materialization, error)
	GetMaterializationsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.Materialization, error)
	ListMaterializations(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.Materialization, error)
}
*/

type errorListMaterializationClient struct{}

func (*errorListMaterializationClient) CreateMaterialization(ctx context.Context, owner *ent.Owner, newMaterialization *ent.Materialization, dependencies []*ent.MaterializationDependency) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*errorListMaterializationClient) DeleteMaterialization(ctx context.Context, owner *ent.Owner, view *ent.Materialization) error {
	panic("unimplemented")
}

func (*errorListMaterializationClient) GetAllMaterializations(ctx context.Context, owner *ent.Owner) ([]*ent.Materialization, error) {
	panic("unimplemented")
}

func (*errorListMaterializationClient) GetMaterialization(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*errorListMaterializationClient) GetMaterializationByName(ctx context.Context, owner *ent.Owner, name string) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*errorListMaterializationClient) GetMaterializationsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.Materialization, error) {
	return nil, errors.New("some list materializations error")
}

func (*errorListMaterializationClient) GetMaterializationsBySourceType(ctx context.Context, owner *ent.Owner, sourceType materialization.SourceType) ([]*ent.Materialization, error) {
	panic("unimplemented")
}

func (*errorListMaterializationClient) ListMaterializations(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.Materialization, error) {
	panic("unimplemented")
}

func (*errorListMaterializationClient) UpdateDataVersion(ctx context.Context, materialization *ent.Materialization, newDataVersion int64) (*ent.Materialization, error) {
	panic("unimplemented")
}

type emptyListMaterializationClient struct{}

func (*emptyListMaterializationClient) CreateMaterialization(ctx context.Context, owner *ent.Owner, newMaterialization *ent.Materialization, dependencies []*ent.MaterializationDependency) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*emptyListMaterializationClient) DeleteMaterialization(ctx context.Context, owner *ent.Owner, view *ent.Materialization) error {
	panic("unimplemented")
}

func (*emptyListMaterializationClient) GetAllMaterializations(ctx context.Context, owner *ent.Owner) ([]*ent.Materialization, error) {
	panic("unimplemented")
}

func (*emptyListMaterializationClient) GetMaterialization(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*emptyListMaterializationClient) GetMaterializationByName(ctx context.Context, owner *ent.Owner, name string) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*emptyListMaterializationClient) GetMaterializationsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.Materialization, error) {
	return []*ent.Materialization{}, nil
}

func (*emptyListMaterializationClient) GetMaterializationsBySourceType(ctx context.Context, owner *ent.Owner, sourceType materialization.SourceType) ([]*ent.Materialization, error) {
	panic("unimplemented")
}

func (*emptyListMaterializationClient) ListMaterializations(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.Materialization, error) {
	return nil, errors.New("some list materializations error")
}

func (*emptyListMaterializationClient) UpdateDataVersion(ctx context.Context, materialization *ent.Materialization, newDataVersion int64) (*ent.Materialization, error) {
	panic("unimplemented")
}

type singleListMaterializationClient struct{}

func (*singleListMaterializationClient) CreateMaterialization(ctx context.Context, owner *ent.Owner, newMaterialization *ent.Materialization, dependencies []*ent.MaterializationDependency) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*singleListMaterializationClient) DeleteMaterialization(ctx context.Context, owner *ent.Owner, view *ent.Materialization) error {
	panic("unimplemented")
}

func (*singleListMaterializationClient) GetAllMaterializations(ctx context.Context, owner *ent.Owner) ([]*ent.Materialization, error) {
	panic("unimplemented")
}

func (*singleListMaterializationClient) GetMaterialization(ctx context.Context, owner *ent.Owner, id uuid.UUID) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*singleListMaterializationClient) GetMaterializationByName(ctx context.Context, owner *ent.Owner, name string) (*ent.Materialization, error) {
	panic("unimplemented")
}

func (*singleListMaterializationClient) GetMaterializationsWithDependency(ctx context.Context, owner *ent.Owner, name string, dependencyType schema.DependencyType) ([]*ent.Materialization, error) {
	return []*ent.Materialization{
		{
			ID:   uuid.MustParse(materializationUUID),
			Name: sampleMaterialization,
			Analysis: &pb.Analysis{
				FreeNames: []string{sampleTable},
			},
		},
	}, nil
}

func (*singleListMaterializationClient) GetMaterializationsBySourceType(ctx context.Context, owner *ent.Owner, sourceType materialization.SourceType) ([]*ent.Materialization, error) {
	panic("unimplemented")
}


func (*singleListMaterializationClient) ListMaterializations(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int) ([]*ent.Materialization, error) {
	return nil, errors.New("some list materializations error")
}

func (*singleListMaterializationClient) UpdateDataVersion(ctx context.Context, materialization *ent.Materialization, newDataVersion int64) (*ent.Materialization, error) {
	panic("unimplemented")
}
