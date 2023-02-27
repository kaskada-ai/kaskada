// based on https://github.com/onsi/gomega/blob/master/matchers/have_occurred_matcher.go
// has updated output formatting to handle grpc.status errors

package matchers

import (
	"fmt"
	"reflect"

	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	grpcStatus "google.golang.org/grpc/status"
)

type status struct {
	Code    string   `json:"code,omitempty"`
	Message string   `json:"message,omitempty"`
	Details []string `json:"details,omitempty"`
}

func isError(a interface{}) bool {
	_, ok := a.(error)
	return ok
}

func isNil(a interface{}) bool {
	if a == nil {
		return true
	}

	switch reflect.TypeOf(a).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflect.ValueOf(a).IsNil()
	}

	return false
}

func HaveOccurredGrpc() types.GomegaMatcher {
	return &haveOccurredGrpcMatcher{}
}

type haveOccurredGrpcMatcher struct {
}

func (matcher *haveOccurredGrpcMatcher) Match(actual interface{}) (success bool, err error) {
	// is purely nil?
	if actual == nil {
		return false, nil
	}

	// must be an 'error' type
	if !isError(actual) {
		return false, fmt.Errorf("expected an error-type.  Got:\n%s", format.Object(actual, 1))
	}

	// must be non-nil (or a pointer to a non-nil)
	return !isNil(actual), nil
}

func (matcher *haveOccurredGrpcMatcher) FailureMessage(actual interface{}) (message string) {
	status := getStatus(actual)
	if status == nil {
		return fmt.Sprintf("expected an error to have occurred.  Got:\n%s", format.Object(actual, 1))
	} else {
		return fmt.Sprintf("expected an error to have occurred.  Got:\n%s", format.Object(status, 1))
	}
}

func (matcher *haveOccurredGrpcMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	status := getStatus(actual)
	if status == nil {
		return fmt.Sprintf("unexpected error:\n%s\n%s\n%s", format.Object(actual, 1), format.IndentString(actual.(error).Error(), 1), "occurred")
	} else {
		return fmt.Sprintf("unexpected error:\n%s\n%s", format.Object(status, 1), "occurred")
	}
}

func getStatus(actual interface{}) interface{} {
	if err, ok := actual.(error); ok {
		if statusErr, ok := grpcStatus.FromError(err); ok {
			s := status{
				Code:    statusErr.Code().String(),
				Message: statusErr.Message(),
				Details: []string{},
			}

			for _, detail := range statusErr.Proto().GetDetails() {
				// the `detail.String()` method uses the global type registry to resolve the proto
				// message type and unmarshal it into a string. In order for a message type to appear
				// in the global registry, the Go type representing that protobuf message type must
				// be linked into the Go binary. This is achieved through an import of the generated
				// Go package representing a .proto file.
				//
				// The following imports are added with an underscore (see: https://tinyurl.com/n5rxvbky)
				// to make sure they are included in the build:
				//
				// "google.golang.org/genproto/googleapis/rpc/errdetails"
				s.Details = append(s.Details, detail.String())
			}
			return s
		}
	}
	return nil
}

/* (Failed) Experiments unmarshaling type Any protobufs more cleanly:
if statusErr, ok := grpcStatus.FromError(err); ok {
	s := status{
		Code:    statusErr.Code().String(),
		Message: statusErr.Message(),
		Details: []interface{}{},
	}

	for _, detail := range statusErr.Proto().GetDetails() {
			// the `any.UnmarshalNew` method uses the global type registry to resolve the proto
			// message type and unmarshal it into a object. In order for a message type to appear
			// in the global registry, the Go type representing that protobuf message type must
			// be linked into the Go binary. This is achieved through an import of the generated
			// Go package representing a .proto file.
			//
			// The following imports are added with an underscore (see: https://tinyurl.com/n5rxvbky)
			// to make sure they are included in the build:
			//
			// "google.golang.org/genproto/googleapis/rpc/errdetails"
		m, err := any.UnmarshalNew(detail, proto.UnmarshalOptions{})
		if err != nil {
			// if this error occurs, an additional import needs to be added, so that the local go
			// registry includes the proper type
			log.Ctx(ctx).Error().Err(err).Str("detail type", detail.GetTypeUrl()).Msg("unable to unmarshal error status")
			// include minimal details as-is
			s.Details = append(s.Details, detail.String())
		} else {
			// add string details for debugging output
			s.Details = append(s.Details, detail.String())
			// attempt to find properties/methods to re-compose the proto structure
			s.Details = append(s.Details, m.ProtoReflect().Descriptor().FullName())
			for fieldNum := 0; fieldNum < m.ProtoReflect().Descriptor().Fields().Len(); fieldNum++ {
				s.Details = append(s.Details, m.ProtoReflect().Get(m.ProtoReflect().Descriptor().Fields().Get(fieldNum)))
			}
		}
	}
	return s
}
*/
