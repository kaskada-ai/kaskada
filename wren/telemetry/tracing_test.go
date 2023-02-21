package telemetry

import (
	"reflect"
	"testing"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
)

func Test_addRequestDetailsToResponse(t *testing.T) {
	cases := []struct {
		Name           string
		TraceID        string
		Input          interface{}
		ExpectedOutput interface{}
	}{
		{
			"v1alpha response",
			"trace1",
			&v1alpha.GetTableResponse{},
			&v1alpha.GetTableResponse{
				RequestDetails: &v1alpha.RequestDetails{
					RequestId: "trace1",
				},
			},
		},
		{
			"v2alpha response",
			"trace2",
			&v2alpha.GetQueryResponse{},
			&v2alpha.GetQueryResponse{
				RequestDetails: &v1alpha.RequestDetails{
					RequestId: "trace2",
				},
			},
		},
	}

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			got := addRequestDetailsToResponse(test.Input, test.TraceID)

			if !reflect.DeepEqual(got, test.ExpectedOutput) {
				t.Errorf("got %v, want %v", got, test.ExpectedOutput)
			}
		})
	}
}
