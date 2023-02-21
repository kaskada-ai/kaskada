package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"

	v1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
	v2alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v2alpha"
	"github.com/kaskada-ai/kaskada/wren/property"
)

// KaskadaQuery holds the details provided at query time.
type KaskadaQuery struct {
	ent.Schema
}

// Fields of the KaskadaQuery.
func (KaskadaQuery) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).Default(uuid.New),
		field.Time("created_at").Default(time.Now).Immutable(),
		//updated_at needs to be nillable, because the table was initially created without this field
		field.Time("updated_at").Default(microsecondNow).UpdateDefault(microsecondNow).Optional().Nillable(),
		field.String("expression").Immutable(),
		field.UUID("data_token_id", uuid.UUID{}).Immutable(),
		field.Bytes("query").GoType(&v1alpha.Query{}).Immutable().Optional().Nillable(),
		field.Bytes("views").GoType(&v2alpha.QueryViews{}).Immutable().Optional().Nillable(),
		field.Bytes("config").GoType(&v2alpha.QueryConfig{}).Immutable().Optional().Nillable(),
		field.Enum("state").GoType(property.QueryState("")).Default(string(property.QueryStateUnspecified)),
		field.Bytes("metrics").GoType(&v2alpha.QueryMetrics{}).Optional().Nillable(),
		field.Bytes("compile_response").GoType(&v1alpha.CompileResponse{}).Optional().Nillable(),

		//TODO: make a join table to handle related prepare jobs for durable queries.  sqlite doesn't have a UUIDarray type.
		//field.Other("related_prepare_jobs", &pgtype.UUIDArray{}).SchemaType(map[string]string{dialect.Postgres: "uuid[]"}).Optional(),
	}
}

// Edges of KaskadaQuery.
func (KaskadaQuery) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("owner", Owner.Type).Ref("kaskada_queries").Unique().Required(),
		edge.To("kaskada_query_results", KaskadaQueryResult.Type).Annotations(entsql.Annotation{OnDelete: entsql.Cascade}),
	}
}
