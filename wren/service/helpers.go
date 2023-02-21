package service

import (
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/customerrors"
)

func wrapErrorWithStatus(err error, subLogger zerolog.Logger) error {
	if err != nil {

		//if err object is already a status error
		if statusErr, ok := status.FromError(err); ok {
			// if passed an internal error, mask the details and message from the user
			if statusErr.Code() == codes.Internal {
				return status.Error(codes.Internal, "internal error")
			}
			// otherwise return status error as-is
			return err
		}

		switch originalErr := errors.Cause(err).(type) {
		case *customerrors.AlreadyExistsError:
			subLogger.Warn().Err(err).Stack().Msg("already exists")
			return status.Error(codes.AlreadyExists, originalErr.Error())
		case *customerrors.ComputeError:
			statusErr, ok := status.FromError(originalErr.Err)
			if ok {
				subLogger.Warn().Interface("code", statusErr.Code()).Str("msg", statusErr.Message()).Interface("details", statusErr.Details()).Msg("compute backend error")
				return statusErr.Err()
			} else {
				subLogger.Warn().Err(err).Msg("other compute backend error")
				return status.Error(codes.Internal, "internal compute error")
			}
		case *customerrors.FailedPreconditionError:
			subLogger.Warn().Err(err).Stack().Msg("failed precondition")
			failedPrecondition := status.New(codes.FailedPrecondition, originalErr.Error())
			for _, violation := range originalErr.Violations {
				updatedFailedPrecondition, err := failedPrecondition.WithDetails(&errdetails.PreconditionFailure_Violation{
					Type:        violation.Kind,
					Subject:     violation.Subject,
					Description: violation.Description,
				})
				if err != nil {
					subLogger.Error().Err(err).Interface("violation", violation).Str("error_msg", originalErr.Error()).Msg("issue adding violation to failedPreconditon error")
					break
				}
				failedPrecondition = updatedFailedPrecondition
			}
			return failedPrecondition.Err()
		case *customerrors.InternalError:
			subLogger.Error().Err(err).Stack().Msg("internal error")
			return status.Error(codes.Internal, "internal error")
		case *customerrors.InvalidArgumentError:
			subLogger.Warn().Err(err).Stack().Msg("invalid argument")
			return status.Error(codes.InvalidArgument, originalErr.Error())
		case *customerrors.NotFoundError:
			subLogger.Warn().Err(err).Stack().Msg("not found")
			return status.Error(codes.NotFound, originalErr.Error())
		case *customerrors.ResourceExhaustedError:
			subLogger.Warn().Err(err).Stack().Msg("resource exhausted")
			return status.Error(codes.ResourceExhausted, originalErr.Error())
		case *customerrors.PermissionDeniedError:
			subLogger.Warn().Err(err).Stack().Msg("permission denied")
			return status.Error(codes.PermissionDenied, originalErr.Error())
		case *customerrors.UnimplementedError:
			subLogger.Warn().Err(err).Stack().Msg("unimplemented")
			return status.Error(codes.Unimplemented, originalErr.Error())
		default:
			subLogger.Error().Err(err).Stack().Msgf("unexpected error")
			return status.Error(codes.Internal, "internal error")
		}
	}
	return nil
}

func getAnalysisFromCompileResponse(compileResp *v1alpha.CompileResponse) *v1alpha.Analysis {
	return &v1alpha.Analysis{
		CanExecute:      compileResp.Plan != nil,
		FenlDiagnostics: compileResp.FenlDiagnostics,
		FreeNames:       compileResp.FreeNames,
		MissingNames:    compileResp.MissingNames,
	}
}
