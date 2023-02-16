package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// PreparedFile holds the schema definition for the PreparedFile entity.
type PreparedFile struct {
	ent.Schema
}

// Fields of the PreparedFile.
func (PreparedFile) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.String("path").Immutable(),
		field.Int64("min_event_time").Immutable().Comment("the min event time in the file, stored as an int64 representing the timestamp in nanoseconds"),
		field.Int64("max_event_time").Immutable().Comment("the max event time in the file, stored as an int64 representing the timestamp in nanoseconds"),
		field.Int64("row_count").Immutable(),
		field.Int64("valid_from_version").Immutable().Comment("the (incremental) data version id that this file is valid FROM"),
		field.Int64("valid_to_version").Optional().Nillable().Comment("the (incremental) data version id that this file is valid TO"),
		field.String("metadata_path").Optional().Nillable().Immutable(),
	}
}

// Edges of the PreparedFile.
func (PreparedFile) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("kaskada_table", KaskadaTable.Type).Ref("prepared_files").Unique().Required(),
		edge.From("prepare_job", PrepareJob.Type).Ref("prepared_files").Unique().Required(),
	}
}
