package matchers

import (
	"fmt"

	"github.com/onsi/gomega/types"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

func MaterializationMatcher(expected interface{}) types.GomegaMatcher {
	return &materializationMatcher{
		expected: expected,
	}
}

type materializationMatcher struct {
	expected interface{}
}

func (matcher *materializationMatcher) Match(actual interface{}) (success bool, err error) {
	expectedMaterialization, ok := matcher.expected.(*v1alpha.Materialization)
	if !ok {
		return false, fmt.Errorf("MaterializationMatcher expected should be an apiv1alpha.Materialization")
	}

	actualMaterialization, ok := actual.(*v1alpha.Materialization)
	if !ok {
		return false, fmt.Errorf("MaterializationMatcher actual should be an apiv1alpha.Materialization")
	}

	switch {
	case expectedMaterialization.MaterializationId != actualMaterialization.MaterializationId:
		return false, fmt.Errorf("expected id: %s does not match actual id: %s", expectedMaterialization.MaterializationId, actualMaterialization.MaterializationId)
	case expectedMaterialization.MaterializationName != actualMaterialization.MaterializationName:
		return false, fmt.Errorf("expected name: %s does not match actual name: %s", expectedMaterialization.MaterializationName, actualMaterialization.MaterializationName)
	//case expectedMaterialization.Schema != actualMaterialization.Schema:
	//	return false, fmt.Errorf("expected schema: %s does not match actual schema: %s", expectedMaterialization.Schema, actualMaterialization.Schema)
	//case expectedMaterialization.Destination != actualMaterialization.Destination:
	//	return false, fmt.Errorf("expected Destination: %s does not match actual Destination: %s", expectedMaterialization.Destination, actualMaterialization.Destination)
	//case expectedMaterialization.CreateTime != actualMaterialization.CreateTime:
	//	return false, fmt.Errorf("expected CreateTime: %s does not match actual CreateTime: %s", expectedMaterialization.CreateTime, actualMaterialization.CreateTime)
	case expectedMaterialization.Query != actualMaterialization.Query:
		return false, fmt.Errorf("expected Query: %s does not match actual Query: %s", expectedMaterialization.Query, actualMaterialization.Query)
	//case expectedMaterialization.Analysis != actualMaterialization.Analysis:
	//	return false, fmt.Errorf("expected Analysis: %s does not match actual Analysis: %s", expectedMaterialization.Analysis, actualMaterialization.Analysis)
	//case expectedMaterialization.Slice != actualMaterialization.Slice:
	//	return false, fmt.Errorf("expected Slice: %s does not match actual Slice: %s", expectedMaterialization.Slice, actualMaterialization.Slice)
	default:
		return true, nil
	}

}

func (matcher *materializationMatcher) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nto match \n\t%#v.  Issue: %s", actual, matcher.expected, message)
}

func (matcher *materializationMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nnot to match \n\t%#v.  Issue: %s", actual, matcher.expected, message)
}
