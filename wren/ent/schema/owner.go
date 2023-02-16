package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// Owner holds the schema definition for the Owner entity.
type Owner struct {
	ent.Schema
}

// Fields of the Owner.
func (Owner) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.String("name").Optional().Nillable(),
		field.String("contact").Optional().Nillable(),
		field.String("client_id").Immutable(),
	}
}

// Edges of the Owner.
func (Owner) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("compute_snapshots", ComputeSnapshot.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("data_tokens", DataToken.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("data_versions", DataVersion.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("kaskada_tables", KaskadaTable.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("kaskada_views", KaskadaView.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("materializations", Materialization.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("kaskada_queries", KaskadaQuery.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
	}
}
