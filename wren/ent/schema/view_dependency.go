package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// ViewDependency holds the schema definition for the ViewDependency entity.
type ViewDependency struct {
	ent.Schema
}

// Fields of the ViewDependency.
func (ViewDependency) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Enum("dependency_type").GoType(DependencyType("")).Immutable(),
		field.String("dependency_name").Immutable().Comment("name of the table/view"),
		field.UUID("dependency_id", uuid.UUID{}).Optional().Nillable().Comment("id of the table/view, or nil if the table/view has been deleted"),
	}
}

// Edges of the ViewDependency.
func (ViewDependency) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("kaskada_view", KaskadaView.Type).Ref("dependencies").Unique().Required(),
	}
}
