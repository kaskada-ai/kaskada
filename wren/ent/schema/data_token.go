package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// DataToken holds the schema definition for the DataToken entity.
type DataToken struct {
	ent.Schema
}

// Fields of the DataToken.
func (DataToken) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.Int64("data_version_id").Immutable(),
		field.UUID("kaskada_table_id", uuid.UUID{}).Immutable(),
	}
}

// Edges of the DataToken.
func (DataToken) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("owner", Owner.Type).Ref("data_tokens").Unique().Required(),
	}
}
