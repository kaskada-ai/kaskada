package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/google/uuid"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

// Materialization holds the schema definition for the Materialization entity.
type Materialization struct {
	ent.Schema
}

// Fields of the Materialization.
func (Materialization) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Int64("version"),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.String("name").Immutable(),
		field.String("description").Optional(),
		field.String("expression").Immutable(),
		field.Bytes("with_views").GoType(&v1alpha.WithViews{}).Immutable(),
		field.Bytes("destination").GoType(&v1alpha.Destination{}).Immutable(),
		field.Bytes("schema").GoType(&v1alpha.Schema{}).Immutable(),
		field.Bytes("slice_request").GoType(&v1alpha.SliceRequest{}).Immutable(),
		field.Bytes("analysis").GoType(&v1alpha.Analysis{}).Immutable(),
		field.Int64("data_version_id"),
	}
}

// Edges of the Materialization.
func (Materialization) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("owner", Owner.Type).Ref("materializations").Unique().Required(),
		edge.To("dependencies", MaterializationDependency.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
	}
}

// Indexes of the Materialization.
func (Materialization) Indexes() []ent.Index {
	return []ent.Index{
		// ensure materialization name is unique per owner
		index.Fields("name").Edges("owner").Unique(),
	}
}
