package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/google/uuid"

	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

// KaskadaView holds the schema definition for the KaskadaView entity.
type KaskadaView struct {
	ent.Schema
}

// Fields of the KaskadaView.
func (KaskadaView) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.String("name").Immutable(),
		field.String("description").Optional(),
		field.String("expression").Immutable(),
		field.Bytes("data_type").GoType(&v1alpha.DataType{}).Immutable(),
		field.Bytes("analysis").GoType(&v1alpha.Analysis{}).Immutable(),
	}
}

// Edges of the KaskadaView.
func (KaskadaView) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("owner", Owner.Type).Ref("kaskada_views").Unique().Required(),
		edge.To("dependencies", ViewDependency.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
	}
}

// Indexes of the KaskadaView.
func (KaskadaView) Indexes() []ent.Index {
	return []ent.Index{
		// ensure kaskadaview name is unique per owner
		index.Fields("name").Edges("owner").Unique(),
	}
}
