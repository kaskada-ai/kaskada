package service

import (
	"context"
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

func Test_queryV1Service_getQuery(t *testing.T) {
	type fields struct {
		UnimplementedQueryServiceServer v1alpha.UnimplementedQueryServiceServer
		kaskadaQueryClient              internal.KaskadaQueryClient
	}
	type args struct {
		owner   *ent.Owner
		queryId string
	}
	sampleKaskadaQuery := &v1alpha.Query{
		QueryId:     "e8c1b897-7b37-4773-bd07-44df5eb2b8b9",
		Expression:  "some-fenl-expression",
		Destination: nil,
		DataTokenId: &wrapperspb.StringValue{},
		Views:       []*v1alpha.View{},
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *v1alpha.GetQueryResponse
		wantErr bool
		err     error
	}{
		{
			name: "invalid query id should throw UUID parsing error",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              &MockKaskadaQueryClient{},
			},
			args: args{
				owner:   &ent.Owner{},
				queryId: "some-invalid-id",
			},
			want:    nil,
			wantErr: true,
			err:     errors.New("invalid query_id provided"),
		},
		{
			name: "empty query id should throw UUID parsing error",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              &MockKaskadaQueryClient{},
			},
			args: args{
				owner:   &ent.Owner{},
				queryId: "",
			},
			want:    nil,
			wantErr: true,
			err:     errors.New("invalid query_id provided"),
		},
		{
			name: "invalid uuid as query id should throw UUID parsing error",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              &MockKaskadaQueryClient{},
			},
			args: args{
				owner:   &ent.Owner{},
				queryId: "abcdefgh-abcd-abcd-abcd-abcdefghijkl",
			},
			want:    nil,
			wantErr: true,
			err:     errors.New("invalid query_id provided"),
		},
		{
			name: "returns the results from the query client",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              &MockKaskadaQueryClient{},
			},
			args: args{
				owner:   &ent.Owner{},
				queryId: uuid.New().String(),
			},
			want: &v1alpha.GetQueryResponse{
				Query: sampleKaskadaQuery,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &queryV1Service{
				UnimplementedQueryServiceServer: tt.fields.UnimplementedQueryServiceServer,
				kaskadaQueryClient:              tt.fields.kaskadaQueryClient,
			}
			ctx := context.Background()
			got, err := q.getQuery(ctx, tt.args.owner, tt.args.queryId)
			if err != nil {
				if tt.wantErr {
					if err.Error() != tt.err.Error() {
						t.Errorf("queryService.getQuery() error = %v, want %v", err, tt.err)
					}
				} else {
					t.Errorf("queryService.getQuery() error = %v, wantErr %v", err, tt.err)
					return
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("queryService.getQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_queryV1Service_listQueries(t *testing.T) {
	type fields struct {
		UnimplementedQueryServiceServer v1alpha.UnimplementedQueryServiceServer
		kaskadaQueryClient              internal.KaskadaQueryClient
	}
	type args struct {
		owner     *ent.Owner
		search    string
		pageToken string
		pageSize  int
	}
	invalidListRequest := &v1alpha.ListQueriesRequest{
		Search:    "",
		PageSize:  0,
		PageToken: "something-not-base64-or-numeric",
	}
	invalidListRequestBytes, _ := proto.Marshal(invalidListRequest)
	invalidListRequestB64 := base64.URLEncoding.EncodeToString(invalidListRequestBytes)

	sampleKaskadaQuery := &v1alpha.Query{
		QueryId:     "e8c1b897-7b37-4773-bd07-44df5eb2b8b9",
		Expression:  "some-fenl-expression",
		Destination: nil,
		DataTokenId: &wrapperspb.StringValue{},
		Views:       []*v1alpha.View{},
	}
	listQueries := make([]*v1alpha.Query, 0)
	listQueries = append(listQueries, sampleKaskadaQuery)
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *v1alpha.ListQueriesResponse
		wantErr bool
		err     error
	}{
		{
			name: "should throw an error if unable to parse base64 token",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              nil,
			},
			args: args{
				pageToken: "something-invalid-not-base64",
			},
			want:    nil,
			wantErr: true,
			err:     status.Error(codes.InvalidArgument, "invalid page token"),
		},
		{
			name: "should throw an error if invalid page token is provided",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              nil,
			},
			args: args{
				pageToken: "YXdrd2FyZCB0YWNvcw==",
			},
			want:    nil,
			wantErr: true,
			err:     status.Error(codes.InvalidArgument, "invalid page token"),
		},
		{
			name: "should throw an error if invalid base64 page token sent",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              nil,
			},
			args: args{
				pageToken: invalidListRequestB64,
			},
			want:    nil,
			wantErr: true,
			err:     status.Error(codes.InvalidArgument, "invalid page token"),
		},
		{
			name: "should return the results of list queries",
			fields: fields{
				UnimplementedQueryServiceServer: v1alpha.UnimplementedQueryServiceServer{},
				kaskadaQueryClient:              &MockKaskadaQueryClient{},
			},
			args: args{},
			want: &v1alpha.ListQueriesResponse{
				Queries: listQueries,
			},
			wantErr: false,
			err:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &queryV1Service{
				UnimplementedQueryServiceServer: tt.fields.UnimplementedQueryServiceServer,
				kaskadaQueryClient:              tt.fields.kaskadaQueryClient,
			}
			ctx := context.Background()
			got, err := q.listQueries(ctx, tt.args.owner, tt.args.search, tt.args.pageToken, tt.args.pageSize)
			if err != nil {
				if tt.wantErr {
					if err.Error() != tt.err.Error() {
						t.Errorf("queryService.listQuery() error = %v, want %v", err, tt.err)
					}
				} else {
					t.Errorf("queryService.listQuery() error = %v, wantErr %v", err, tt.err)
					return
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("queryService.listQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

type MockKaskadaQueryClient struct{}

func (m *MockKaskadaQueryClient) CreateKaskadaQuery(ctx context.Context, owner *ent.Owner, newQuery *ent.KaskadaQuery, isV2 bool) (*ent.KaskadaQuery, error) {
	return nil, nil
}

func (m *MockKaskadaQueryClient) DeleteKaskadaQuery(ctx context.Context, owner *ent.Owner, id uuid.UUID, isV2 bool) error {
	return nil
}

func (m *MockKaskadaQueryClient) GetAllKaskadaQueries(ctx context.Context, owner *ent.Owner, isV2 bool) ([]*ent.KaskadaQuery, error) {
	return nil, nil
}

func (m *MockKaskadaQueryClient) GetKaskadaQuery(ctx context.Context, owner *ent.Owner, id uuid.UUID, isV2 bool) (*ent.KaskadaQuery, error) {
	sampleKaskadaQuery := &ent.KaskadaQuery{
		Query: &v1alpha.Query{
			QueryId:     "e8c1b897-7b37-4773-bd07-44df5eb2b8b9",
			Expression:  "some-fenl-expression",
			Destination: nil,
			DataTokenId: &wrapperspb.StringValue{},
			Views:       []*v1alpha.View{},
		},
	}
	return sampleKaskadaQuery, nil
}

func (m *MockKaskadaQueryClient) ListKaskadaQueries(ctx context.Context, owner *ent.Owner, searchTerm string, pageSize int, offset int, isV2 bool) ([]*ent.KaskadaQuery, error) {
	sampleKaskadaQuery := &ent.KaskadaQuery{
		Query: &v1alpha.Query{
			QueryId:     "e8c1b897-7b37-4773-bd07-44df5eb2b8b9",
			Expression:  "some-fenl-expression",
			Destination: nil,
			DataTokenId: &wrapperspb.StringValue{},
			Views:       []*v1alpha.View{},
		},
	}
	queries := make([]*ent.KaskadaQuery, 0)
	queries = append(queries, sampleKaskadaQuery)
	return queries, nil
}
