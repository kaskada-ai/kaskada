package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"

	"github.com/kaskada/kaskada-ai/wren/property"
)

// KaskadaQueryResult holds the details provided at query time.
type KaskadaQueryResult struct {
	ent.Schema
}

// Fields of the KaskadaQueryResult.
func (KaskadaQueryResult) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(time.Now).Immutable(),
		field.String("path").Immutable(),
		field.Enum("type").GoType(property.QueryResultType("")).Default(string(property.QueryResultTypeUnspecified)),
	}
}

// Edges of KaskadaQueryResult.
func (KaskadaQueryResult) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("kaskada_query", KaskadaQuery.Type).Ref("kaskada_query_results").Unique().Required(),
	}
}
