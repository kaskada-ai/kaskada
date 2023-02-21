package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	"github.com/kaskada-ai/kaskada/wren/property"
)

// PrepareJob holds the schema definition for the PrepareJob entity.
type PrepareJob struct {
	ent.Schema
}

// Fields of the PrepareJob.
func (PrepareJob) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		//updated_at needs to be nillable, because the table was initially created without this field
		field.Time("updated_at").Default(microsecondNow).UpdateDefault(microsecondNow).Optional().Nillable(),
		field.Bytes("slice_plan").GoType(&v1alpha.SlicePlan{}).Optional().Nillable(),
		field.Bytes("slice_hash").Immutable(),
		field.Int32("prepare_cache_buster").Immutable(),
		field.Enum("state").GoType(property.PrepareJobState("")).Default(string(property.PrepareJobStateUnspecified)),
	}
}

// Edges of the PrepareJob.
func (PrepareJob) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("kaskada_table", KaskadaTable.Type).Ref("prepare_jobs").Unique().Required(),
		edge.To("kaskada_files", KaskadaFile.Type),
		edge.To("prepared_files", PreparedFile.Type),
	}
}
