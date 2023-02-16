package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// ComputeSnapshot holds the schema definition for the ComputeSnapshot entity.
type ComputeSnapshot struct {
	ent.Schema
}

// Fields of the ComputeSnapshot.
func (ComputeSnapshot) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(microsecondNow).Immutable(),
		field.Int64("data_version_id").Immutable(),
		field.Int32("snapshot_cache_buster").Immutable(),
		field.Bytes("plan_hash").Immutable(),
		field.Int64("max_event_time").Immutable().Comment("the max event time included in the snapshot, stored as an int64 representing the timestamp in nanoseconds"),
		field.String("path").Immutable().Comment("the path where the snapshot files are stored"),
	}
}

// Edges of the ComputeSnapshot.
func (ComputeSnapshot) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("kaskada_tables", KaskadaTable.Type).Ref("compute_snapshots").Required(),
		edge.From("owner", Owner.Type).Ref("compute_snapshots").Unique().Required(),
	}
}
