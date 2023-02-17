package compute

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kaskada-ai/kaskada/wren/internal"
)

func TestGetTableIDs(t *testing.T) {
	t.Run("should get the proper table ids", func(t *testing.T) {
		one := uuid.New()
		two := uuid.New()

		tables := map[uuid.UUID]*internal.SliceTable{
			one: nil,
			two: nil,
		}
		queryContext, queryContextCancel := GetNewQueryContext(context.Background(), nil, nil, nil, nil, nil, false, nil, nil, nil, tables)
		defer queryContextCancel()
		tableIDs := queryContext.GetTableIDs()

		isOneFound := false
		isTwoFound := false
		for _, id := range tableIDs {
			if id == one {
				isOneFound = true
			} else if id == two {
				isTwoFound = true
			}
		}
		assert.True(t, isOneFound, "unable to find first table ID")
		assert.True(t, isTwoFound, "unable to find second table ID")
	})
}
