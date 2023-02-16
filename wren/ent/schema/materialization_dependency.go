package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// MaterializationDependency holds the schema definition for the MaterializationDependency entity.
type MaterializationDependency struct {
	ent.Schema
}

// Fields of the MaterializationDependency.
func (MaterializationDependency) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Enum("dependency_type").GoType(DependencyType("")).Immutable(),
		field.String("dependency_name").Immutable().Comment("name of the table/view"),
		field.UUID("dependency_id", uuid.UUID{}).Optional().Nillable().Comment("id of the table/view, or nil if the table/view has been deleted"),
	}
}

// Edges of the MaterializationDependency.
func (MaterializationDependency) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("materialization", Materialization.Type).Ref("dependencies").Unique().Required(),
	}
}
