package service

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

func Test_materialization_create(t *testing.T) {
	objectStoreDestination := &v1alpha.Destination{
		Destination: &v1alpha.Destination_ObjectStore{
			ObjectStore: &v1alpha.ObjectStoreDestination{
				FileType:        v1alpha.FileType_FILE_TYPE_CSV,
				OutputPrefixUri: "gs://some-bucket/some-prefix",
			},
		},
	}
	pulsarDestination := &v1alpha.Destination{
		Destination: &v1alpha.Destination_Pulsar{
			Pulsar: &v1alpha.PulsarDestination{
				Config: &v1alpha.PulsarConfig{},
			},
		},
	}

	type fields struct {
		v1alpha.UnimplementedMaterializationServiceServer
		kaskadaTableClient    internal.KaskadaTableClient
		kaskadaViewClient     internal.KaskadaViewClient
		dataTokenClient       internal.DataTokenClient
		materializationClient internal.MaterializationClient
		computeManager        compute.ComputeManager
	}
	type args struct {
		owner                       *ent.Owner
		createMaterailzationRequest *v1alpha.CreateMaterializationRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *v1alpha.CreateMaterializationResponse
		wantErr bool
		err     error
	}{
		{
			name: "missing materialization should throw error",
			fields: fields{
				UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
			},
			args: args{
				owner:                       &ent.Owner{},
				createMaterailzationRequest: &v1alpha.CreateMaterializationRequest{},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf("missing materialization definition"),
		},
		{
			name: "missing expression should throw error",
			fields: fields{
				UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
			},
			args: args{
				owner: &ent.Owner{},
				createMaterailzationRequest: &v1alpha.CreateMaterializationRequest{
					Materialization: &v1alpha.Materialization{
						Expression:  "",
						Destination: objectStoreDestination,
					},
				},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf("missing materialization expression"),
		},
		{
			name: "missing destination should throw error",
			fields: fields{
				UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
			},
			args: args{
				owner: &ent.Owner{},
				createMaterailzationRequest: &v1alpha.CreateMaterializationRequest{
					Materialization: &v1alpha.Materialization{
						Expression: "nachos",
					},
				},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf("missing materialization destination"),
		},
		{
			name: "compile error should throw error",
			fields: fields{
				UnimplementedMaterializationServiceServer: v1alpha.UnimplementedMaterializationServiceServer{},
				computeManager: compute.NewComputeManagerMock(compute.GetComputeManagerMockConfig()),
			},
			args: args{
				owner: &ent.Owner{},
				createMaterailzationRequest: &v1alpha.CreateMaterializationRequest{
					Materialization: &v1alpha.Materialization{
						Expression:  "nachos",
						Destination: pulsarDestination,
					},
				},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf("missing materialization destination"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &materializationService{
				UnimplementedMaterializationServiceServer: tt.fields.UnimplementedMaterializationServiceServer,
				kaskadaTableClient:                        tt.fields.kaskadaTableClient,
				kaskadaViewClient:                         tt.fields.kaskadaViewClient,
				dataTokenClient:                           tt.fields.dataTokenClient,
				materializationClient:                     tt.fields.materializationClient,
				computeManager:                            tt.fields.computeManager,
			}
			ctx := context.Background()
			got, err := q.createMaterialization(ctx, tt.args.owner, tt.args.createMaterailzationRequest)
			if err != nil {
				if tt.wantErr {
					if err.Error() != tt.err.Error() {
						t.Errorf("materializationService.CreateMaterialization() error = %v, want %v", err, tt.err)
					}
				} else {
					t.Errorf("materializationService.CreateMaterialization() error = %v, wantErr %v", err, tt.err)
					return
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("materializationService.CreateMaterialization() = %v, want %v", got, tt.want)
			}
		})
	}
}
