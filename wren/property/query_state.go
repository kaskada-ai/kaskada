package property

type QueryState string

const (
	QueryStateUnspecified QueryState = "UNSPECIFIED"
	QueryStateCompiled    QueryState = "COMPILED"
	QueryStatePreparing   QueryState = "PREPARING"
	QueryStatePrepared    QueryState = "PREPARED"
	QueryStateComputing   QueryState = "COMPUTING"
	QueryStateSuccess     QueryState = "SUCCESS"
	QueryStateFailure     QueryState = "FAILURE"
)

// Values provides list valid values for Enum.
func (QueryState) Values() (kinds []string) {
	states := []QueryState{
		QueryStateUnspecified,
		QueryStateCompiled,
		QueryStatePreparing,
		QueryStatePrepared,
		QueryStateComputing,
		QueryStateSuccess,
		QueryStateFailure,
	}

	for _, s := range states {
		kinds = append(kinds, string(s))
	}
	return
}
