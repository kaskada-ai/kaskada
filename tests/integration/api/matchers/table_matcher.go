package matchers

import (
	"fmt"

	"github.com/onsi/gomega/types"

	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

func TableMatcher(expected interface{}) types.GomegaMatcher {
	return &tableMatcher{
		expected: expected,
	}
}

type tableMatcher struct {
	expected interface{}
}

func (matcher *tableMatcher) Match(actual interface{}) (success bool, err error) {
	expectedTable, ok := matcher.expected.(*v1alpha.Table)
	if !ok {
		return false, fmt.Errorf("TableMatcher expected should be an apiv1alpha.Table")
	}

	actualTable, ok := actual.(*v1alpha.Table)
	if !ok {
		return false, fmt.Errorf("TableMatcher actual should be an apiv1alpha.Table")
	}

	switch {
	case expectedTable.TableId != actualTable.TableId:
		return false, fmt.Errorf("expected id: %s does not match actual id: %s", expectedTable.TableId, actualTable.TableId)
	case expectedTable.TableName != actualTable.TableName:
		return false, fmt.Errorf("expected name: %s does not match actual name: %s", expectedTable.TableName, actualTable.TableName)
	case expectedTable.EntityKeyColumnName != actualTable.EntityKeyColumnName:
		return false, fmt.Errorf("expected entity_key_column_name: %s does not match actual entity_key_column_name: %s", expectedTable.EntityKeyColumnName, actualTable.EntityKeyColumnName)
	default:
		return true, nil
	}

}

func (matcher *tableMatcher) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nto match \n\t%#v.  Issue: %s", actual, matcher.expected, message)
}

func (matcher *tableMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nnot to match \n\t%#v.  Issue: %s", actual, matcher.expected, message)
}
