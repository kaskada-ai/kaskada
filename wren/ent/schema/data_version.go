package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// DataVersion holds the schema definition for the DataVersion entity.
type DataVersion struct {
	ent.Schema
}

// Fields of the DataVersion.
func (DataVersion) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Comment("the internal data_token_id"),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.String("external_revision").Optional().Nillable().Immutable().Comment("the external table revision that was the cause for this new data version"),
	}
}

// Edges of the DataVersion.
func (DataVersion) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("kaskada_table", KaskadaTable.Type).Ref("data_versions").Unique(),
		edge.From("owner", Owner.Type).Ref("data_versions").Unique().Required(),
	}
}
