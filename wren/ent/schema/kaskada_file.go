package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/google/uuid"

	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

// KaskadaFile holds the schema definition for the KaskadaFile entity.
type KaskadaFile struct {
	ent.Schema
}

// Fields of the KaskadaFile.
func (KaskadaFile) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.String("path").Immutable(),
		field.String("identifier").Immutable(),
		field.Int64("valid_from_version").Immutable().Comment("the (incremental) data version id that this file is valid FROM"),
		field.Int64("valid_to_version").Optional().Nillable().Comment("the (incremental) data version id that this file is valid TO"),
		field.Bytes("schema").GoType(&v1alpha.Schema{}).Optional().Nillable(),
		field.Enum("type").Values("unspecified", "csv", "parquet").Default("parquet"),
	}
}

// Edges of the KaskadaFile.
func (KaskadaFile) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("kaskada_table", KaskadaTable.Type).Ref("kaskada_files").Unique().Required(),
		edge.From("prepare_jobs", PrepareJob.Type).Ref("kaskada_files"),
	}
}

// Indexes of the KaskadaFile.
func (KaskadaFile) Indexes() []ent.Index {
	return []ent.Index{
		// ensure identifier is unique per table
		index.Fields("identifier").Edges("kaskada_table").Unique(),
	}
}
