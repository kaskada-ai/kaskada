package service

import (
	"context"
	"encoding/base64"
	"strconv"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/auth"
	"github.com/kaskada-ai/kaskada/wren/compute"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
	"github.com/kaskada-ai/kaskada/wren/ent"
	"github.com/kaskada-ai/kaskada/wren/ent/schema"
	"github.com/kaskada-ai/kaskada/wren/internal"
)

type ViewService interface {
	SetAnalyzer(analyzer Analyzer)
	ListViews(ctx context.Context, request *pb.ListViewsRequest) (*pb.ListViewsResponse, error)
	GetView(ctx context.Context, request *pb.GetViewRequest) (*pb.GetViewResponse, error)
	CreateView(ctx context.Context, request *pb.CreateViewRequest) (*pb.CreateViewResponse, error)
	DeleteView(ctx context.Context, request *pb.DeleteViewRequest) (*pb.DeleteViewResponse, error)
}

type viewService struct {
	pb.UnimplementedViewServiceServer
	computeManager     *compute.Manager
	kaskadaTableClient internal.KaskadaTableClient
	kaskadaViewClient  internal.KaskadaViewClient
	dependencyAnalyzer Analyzer
}

// NewViewService creates a new view service
func NewViewService(computeManager *compute.Manager, kaskadaTableClient *internal.KaskadaTableClient, kaskadaViewClient *internal.KaskadaViewClient, dependencyAnalyzer *Analyzer) *viewService {
	return &viewService{
		computeManager:     computeManager,
		kaskadaTableClient: *kaskadaTableClient,
		kaskadaViewClient:  *kaskadaViewClient,
		dependencyAnalyzer: *dependencyAnalyzer,
	}
}

func (s *viewService) ListViews(ctx context.Context, request *pb.ListViewsRequest) (*pb.ListViewsResponse, error) {
	resp, err := s.listViews(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "viewService.ListViews").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *viewService) listViews(ctx context.Context, owner *ent.Owner, request *pb.ListViewsRequest) (*pb.ListViewsResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "viewService.listViews").Logger()
	var (
		pageSize, offset int
	)

	if request.PageToken != "" {
		// decode and unmarshal request object from page token
		// to use instead of the initially passed request object
		data, err := base64.URLEncoding.DecodeString(request.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}

		innerRequest := &pb.ListViewsRequest{}

		err = proto.Unmarshal(data, innerRequest)
		if err != nil {
			subLogger.Info().AnErr("unmarshal", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}
		pageSize = int(innerRequest.PageSize)
		offset, err = strconv.Atoi(innerRequest.PageToken)
		if err != nil {
			subLogger.Info().AnErr("base64 decode", err).Msg("invalid page token")
			return nil, customerrors.NewInvalidArgumentError("invalid page token")
		}
	} else {
		pageSize = int(request.PageSize)
		offset = 0
	}

	views, err := s.kaskadaViewClient.ListKaskadaViews(ctx, owner, request.Search, pageSize, offset)
	if err != nil {
		return nil, err
	}

	response := &pb.ListViewsResponse{
		Views: make([]*pb.View, 0, len(views)),
	}
	for _, view := range views {
		response.Views = append(response.Views, s.getProtoFromDB(view))
	}

	if len(views) > 0 && len(views) == pageSize {
		nextRequest := &pb.ListViewsRequest{
			Search:    request.Search,
			PageSize:  int32(pageSize),
			PageToken: strconv.Itoa(offset + pageSize),
		}

		data, err := proto.Marshal(nextRequest)
		if err != nil {
			subLogger.Err(err).Msg("issue listing views")
			return nil, customerrors.NewInternalError("issue listing views")
		}

		response.NextPageToken = base64.URLEncoding.EncodeToString(data)
	}

	return response, nil
}

func (s *viewService) GetView(ctx context.Context, request *pb.GetViewRequest) (*pb.GetViewResponse, error) {
	resp, err := s.getView(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "viewService.GetView").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *viewService) getView(ctx context.Context, owner *ent.Owner, request *pb.GetViewRequest) (*pb.GetViewResponse, error) {
	kaskadaView, err := s.kaskadaViewClient.GetKaskadaViewByName(ctx, owner, request.ViewName)
	if err != nil {
		return nil, err
	}
	return &pb.GetViewResponse{View: s.getProtoFromDB(kaskadaView)}, nil
}

func (s *viewService) CreateView(ctx context.Context, request *pb.CreateViewRequest) (*pb.CreateViewResponse, error) {
	resp, err := s.createView(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "viewService.CreateView").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *viewService) createView(ctx context.Context, owner *ent.Owner, request *pb.CreateViewRequest) (*pb.CreateViewResponse, error) {
	subLogger := log.Ctx(ctx).With().Str("method", "viewService.createView").Str("expression", request.View.Expression).Logger()

	compileResp, err := s.computeManager.CompileQuery(ctx, owner, request.View.Expression, []*pb.WithView{}, true, false, nil, pb.Query_RESULT_BEHAVIOR_ALL_RESULTS)
	if err != nil {
		subLogger.Error().Err(err).Msg("issue compiling view")
		return nil, err
	}

	if compileResp.FenlDiagnostics.NumErrors > 0 || request.DryRun {
		// then return view response without saving to DB...
		subLogger.Debug().Msg("not creating view due to diagnostic errors or dry-run")
		return &pb.CreateViewResponse{
			View:     request.View,
			Analysis: getAnalysisFromCompileResponse(compileResp),
		}, nil
	}

	tableMap, err := s.kaskadaTableClient.GetKaskadaTablesFromNames(ctx, owner, compileResp.FreeNames)
	if err != nil {
		return nil, err
	}
	viewMap, err := s.kaskadaViewClient.GetKaskadaViewsFromNames(ctx, owner, compileResp.FreeNames)
	if err != nil {
		return nil, err
	}

	dependencies := make([]*ent.ViewDependency, 0, len(compileResp.FreeNames))

	for _, freeName := range compileResp.FreeNames {
		if table, found := tableMap[freeName]; found {
			dependencies = append(dependencies, &ent.ViewDependency{
				DependencyType: schema.DependencyType_Table,
				DependencyName: table.Name,
				DependencyID:   &table.ID,
			})
		} else if view, found := viewMap[freeName]; found {
			dependencies = append(dependencies, &ent.ViewDependency{
				DependencyType: schema.DependencyType_View,
				DependencyName: view.Name,
				DependencyID:   &view.ID,
			})
		} else {
			subLogger.Error().Str("free_name", freeName).Msg("unable to locate free_name dependency when creating view")
			return nil, customerrors.NewInvalidArgumentErrorWithCustomText("view expression contains unknown dependency")
		}
	}

	newView := &ent.KaskadaView{
		Name:       request.View.ViewName,
		Expression: request.View.Expression,
		DataType:   compileResp.ResultType,
		Analysis:   getAnalysisFromCompileResponse(compileResp),
	}

	kaskadaView, err := s.kaskadaViewClient.CreateKaskadaView(ctx, owner, newView, dependencies)
	if err != nil {
		return nil, err
	}

	return &pb.CreateViewResponse{View: s.getProtoFromDB(kaskadaView), Analysis: kaskadaView.Analysis}, nil
}

func (s *viewService) DeleteView(ctx context.Context, request *pb.DeleteViewRequest) (*pb.DeleteViewResponse, error) {
	resp, err := s.deleteView(ctx, auth.APIOwnerFromContext(ctx), request)
	if err != nil {
		subLogger := log.Ctx(ctx).With().Str("method", "viewService.DeleteView").Logger()
		return nil, wrapErrorWithStatus(err, subLogger)
	}
	return resp, nil
}

func (s *viewService) deleteView(ctx context.Context, owner *ent.Owner, request *pb.DeleteViewRequest) (*pb.DeleteViewResponse, error) {
	kaskadaView, err := s.kaskadaViewClient.GetKaskadaViewByName(ctx, owner, request.ViewName)
	if err != nil {
		return nil, err
	}

	if !request.Force {
		dependencies, err := s.dependencyAnalyzer.Analyze(ctx, owner, request.ViewName, schema.DependencyType_View)
		if err != nil {
			return nil, err
		}

		if len(dependencies.views) > 0 || len(dependencies.materializations) > 0 {
			err := dependencies.ToErrorDetails()
			outStatus := status.Newf(codes.FailedPrecondition, "Unable to delete view. Detected: %d dependencies.", len(dependencies.views)+len(dependencies.materializations))
			details, _ := outStatus.WithDetails(&err)
			return nil, details.Err()
		}
	}

	err = s.kaskadaViewClient.DeleteKaskadaView(ctx, owner, kaskadaView)
	if err != nil {
		return nil, err
	}

	return &pb.DeleteViewResponse{}, nil
}

func (s *viewService) getProtoFromDB(view *ent.KaskadaView) *pb.View {
	return &pb.View{
		ViewId:     view.ID.String(),
		ViewName:   view.Name,
		Expression: view.Expression,
		ResultType: view.DataType,
		Analysis:   view.Analysis,
	}
}
