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

// KaskadaTable holds the schema definition for the KaskadaTable entity.
type KaskadaTable struct {
	ent.Schema
}

// Fields of the KaskadaTable.
func (KaskadaTable) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.Time("updated_at").Default(microsecondNow).UpdateDefault(microsecondNow),
		field.String("name").Immutable(),
		field.String("description").Optional(),
		field.String("entity_key_column_name").Immutable(),
		field.String("time_column_name").Immutable(),
		field.String("subsort_column_name").Optional().Nillable().Immutable(),
		field.String("grouping_id").Optional().Immutable(),
		field.Bytes("source").GoType(&v1alpha.Table_TableSource{}).Immutable(),
		field.Bytes("merged_schema").GoType(&v1alpha.Schema{}).Optional().Nillable(),
	}
}

// Edges of the KaskadaTable.
func (KaskadaTable) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("owner", Owner.Type).Ref("kaskada_tables").Unique().Required(),
		edge.To("compute_snapshots", ComputeSnapshot.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("data_versions", DataVersion.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("kaskada_files", KaskadaFile.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("prepare_jobs", PrepareJob.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
		edge.To("prepared_files", PreparedFile.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
	}
}

// Indexes of the KaskadaTable.
func (KaskadaTable) Indexes() []ent.Index {
	return []ent.Index{
		// ensure table name is unique per owner
		index.Fields("name").Edges("owner").Unique(),
	}
}
