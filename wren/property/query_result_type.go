package property

type QueryResultType string

const (
	QueryResultTypeUnspecified QueryResultType = "UNSPECIFIED"
	QueryResultTypeParquet     QueryResultType = "PARQUET"
	QueryResultTypeRedisBulk   QueryResultType = "REDIS_BULK"
)

// Values provides list valid values for Enum.
func (QueryResultType) Values() (kinds []string) {
	resulttypes := []QueryResultType{
		QueryResultTypeUnspecified,
		QueryResultTypeParquet,
		QueryResultTypeRedisBulk,
	}

	for _, s := range resulttypes {
		kinds = append(kinds, string(s))
	}
	return
}
