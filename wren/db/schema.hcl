table "compute_snapshots" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "data_version_id" {
    null = false
    type = bigint
  }
  column "snapshot_cache_buster" {
    null = false
    type = integer
  }
  column "plan_hash" {
    null = false
    type = bytea
  }
  column "max_event_time" {
    null = false
    type = bigint
  }
  column "path" {
    null = false
    type = character_varying
  }
  column "owner_compute_snapshots" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "compute_snapshots_owners_compute_snapshots" {
    columns     = [column.owner_compute_snapshots]
    ref_columns = [table.owners.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "data_tokens" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "data_version_id" {
    null = false
    type = bigint
  }
  column "kaskada_table_id" {
    null = false
    type = uuid
  }
  column "owner_data_tokens" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "data_tokens_owners_data_tokens" {
    columns     = [column.owner_data_tokens]
    ref_columns = [table.owners.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "data_versions" {
  schema = schema.public
  column "id" {
    null = false
    type = bigint
    identity {
      generated = BY_DEFAULT
    }
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "external_revision" {
    null = true
    type = character_varying
  }
  column "kaskada_table_data_versions" {
    null = true
    type = uuid
  }
  column "owner_data_versions" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "data_versions_kaskada_tables_data_versions" {
    columns     = [column.kaskada_table_data_versions]
    ref_columns = [table.kaskada_tables.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  foreign_key "data_versions_owners_data_versions" {
    columns     = [column.owner_data_versions]
    ref_columns = [table.owners.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "kaskada_files" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "path" {
    null = false
    type = character_varying
  }
  column "identifier" {
    null = false
    type = character_varying
  }
  column "valid_from_version" {
    null = false
    type = bigint
  }
  column "valid_to_version" {
    null = true
    type = bigint
  }
  column "schema" {
    null = true
    type = bytea
  }
  column "type" {
    null    = false
    type    = character_varying
    default = "parquet"
  }
  column "kaskada_table_kaskada_files" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "kaskada_files_kaskada_tables_kaskada_files" {
    columns     = [column.kaskada_table_kaskada_files]
    ref_columns = [table.kaskada_tables.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  index "kaskadafile_identifier_kaskada_table_kaskada_files" {
    unique  = true
    columns = [column.identifier, column.kaskada_table_kaskada_files]
    type    = BTREE
  }
}
table "kaskada_queries" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "updated_at" {
    null = true
    type = timestamptz
  }
  column "expression" {
    null = false
    type = character_varying
  }
  column "data_token_id" {
    null = false
    type = uuid
  }
  column "query" {
    null = true
    type = bytea
  }
  column "views" {
    null = true
    type = bytea
  }
  column "config" {
    null = true
    type = bytea
  }
  column "state" {
    null    = false
    type    = character_varying
    default = "UNSPECIFIED"
  }
  column "metrics" {
    null = true
    type = bytea
  }
  column "compile_response" {
    null = true
    type = bytea
  }
  column "owner_kaskada_queries" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "kaskada_queries_owners_kaskada_queries" {
    columns     = [column.owner_kaskada_queries]
    ref_columns = [table.owners.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "kaskada_query_results" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "path" {
    null = false
    type = character_varying
  }
  column "type" {
    null    = false
    type    = character_varying
    default = "UNSPECIFIED"
  }
  column "kaskada_query_kaskada_query_results" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "kaskada_query_results_kaskada_queries_kaskada_query_results" {
    columns     = [column.kaskada_query_kaskada_query_results]
    ref_columns = [table.kaskada_queries.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "kaskada_table_compute_snapshots" {
  schema = schema.public
  column "kaskada_table_id" {
    null = false
    type = uuid
  }
  column "compute_snapshot_id" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.kaskada_table_id, column.compute_snapshot_id]
  }
  foreign_key "kaskada_table_compute_snapshots_compute_snapshot_id" {
    columns     = [column.compute_snapshot_id]
    ref_columns = [table.compute_snapshots.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  foreign_key "kaskada_table_compute_snapshots_kaskada_table_id" {
    columns     = [column.kaskada_table_id]
    ref_columns = [table.kaskada_tables.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "kaskada_tables" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "updated_at" {
    null = false
    type = timestamptz
  }
  column "name" {
    null = false
    type = character_varying
  }
  column "description" {
    null = true
    type = character_varying
  }
  column "entity_key_column_name" {
    null = false
    type = character_varying
  }
  column "time_column_name" {
    null = false
    type = character_varying
  }
  column "subsort_column_name" {
    null = true
    type = character_varying
  }
  column "grouping_id" {
    null = true
    type = character_varying
  }
  column "source" {
    null = false
    type = bytea
  }
  column "merged_schema" {
    null = true
    type = bytea
  }
  column "owner_kaskada_tables" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "kaskada_tables_owners_kaskada_tables" {
    columns     = [column.owner_kaskada_tables]
    ref_columns = [table.owners.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  index "kaskadatable_name_owner_kaskada_tables" {
    unique  = true
    columns = [column.name, column.owner_kaskada_tables]
    type    = BTREE
  }
}
table "kaskada_views" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "name" {
    null = false
    type = character_varying
  }
  column "description" {
    null = true
    type = character_varying
  }
  column "expression" {
    null = false
    type = character_varying
  }
  column "data_type" {
    null = false
    type = bytea
  }
  column "analysis" {
    null = false
    type = bytea
  }
  column "owner_kaskada_views" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "kaskada_views_owners_kaskada_views" {
    columns     = [column.owner_kaskada_views]
    ref_columns = [table.owners.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  index "kaskadaview_name_owner_kaskada_views" {
    unique  = true
    columns = [column.name, column.owner_kaskada_views]
    type    = BTREE
  }
}
table "materialization_dependencies" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "dependency_type" {
    null = false
    type = character_varying
  }
  column "dependency_name" {
    null = false
    type = character_varying
  }
  column "dependency_id" {
    null = true
    type = uuid
  }
  column "materialization_dependencies" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "materialization_dependencies_materializations_dependencies" {
    columns     = [column.materialization_dependencies]
    ref_columns = [table.materializations.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "materializations" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "name" {
    null = false
    type = character_varying
  }
  column "description" {
    null = true
    type = character_varying
  }
  column "expression" {
    null = false
    type = character_varying
  }
  column "with_views" {
    null = false
    type = bytea
  }
  column "destination" {
    null = false
    type = bytea
  }
  column "schema" {
    null = false
    type = bytea
  }
  column "slice_request" {
    null = false
    type = bytea
  }
  column "analysis" {
    null = false
    type = bytea
  }
  column "owner_materializations" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "materializations_owners_materializations" {
    columns     = [column.owner_materializations]
    ref_columns = [table.owners.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  index "materialization_name_owner_materializations" {
    unique  = true
    columns = [column.name, column.owner_materializations]
    type    = BTREE
  }
}
table "owners" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "name" {
    null = true
    type = character_varying
  }
  column "contact" {
    null = true
    type = character_varying
  }
  column "client_id" {
    null = false
    type = character_varying
  }
  primary_key {
    columns = [column.id]
  }
}
table "prepare_job_kaskada_files" {
  schema = schema.public
  column "prepare_job_id" {
    null = false
    type = uuid
  }
  column "kaskada_file_id" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.prepare_job_id, column.kaskada_file_id]
  }
  foreign_key "prepare_job_kaskada_files_kaskada_file_id" {
    columns     = [column.kaskada_file_id]
    ref_columns = [table.kaskada_files.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  foreign_key "prepare_job_kaskada_files_prepare_job_id" {
    columns     = [column.prepare_job_id]
    ref_columns = [table.prepare_jobs.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "prepare_jobs" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "updated_at" {
    null = true
    type = timestamptz
  }
  column "slice_plan" {
    null = true
    type = bytea
  }
  column "slice_hash" {
    null = false
    type = bytea
  }
  column "prepare_cache_buster" {
    null = false
    type = integer
  }
  column "state" {
    null    = false
    type    = character_varying
    default = "UNSPECIFIED"
  }
  column "kaskada_table_prepare_jobs" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "prepare_jobs_kaskada_tables_prepare_jobs" {
    columns     = [column.kaskada_table_prepare_jobs]
    ref_columns = [table.kaskada_tables.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
table "prepared_files" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "created_at" {
    null = false
    type = timestamptz
  }
  column "path" {
    null = false
    type = character_varying
  }
  column "min_event_time" {
    null = false
    type = bigint
  }
  column "max_event_time" {
    null = false
    type = bigint
  }
  column "row_count" {
    null = false
    type = bigint
  }
  column "valid_from_version" {
    null = false
    type = bigint
  }
  column "valid_to_version" {
    null = true
    type = bigint
  }
  column "metadata_path" {
    null = true
    type = character_varying
  }
  column "kaskada_table_prepared_files" {
    null = false
    type = uuid
  }
  column "prepare_job_prepared_files" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "prepared_files_kaskada_tables_prepared_files" {
    columns     = [column.kaskada_table_prepared_files]
    ref_columns = [table.kaskada_tables.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  foreign_key "prepared_files_prepare_jobs_prepared_files" {
    columns     = [column.prepare_job_prepared_files]
    ref_columns = [table.prepare_jobs.column.id]
    on_update   = NO_ACTION
    on_delete   = NO_ACTION
  }
}
table "schema_migrations" {
  schema = schema.public
  column "version" {
    null = false
    type = bigint
  }
  column "dirty" {
    null = false
    type = boolean
  }
  primary_key {
    columns = [column.version]
  }
}
table "view_dependencies" {
  schema = schema.public
  column "id" {
    null = false
    type = uuid
  }
  column "dependency_type" {
    null = false
    type = character_varying
  }
  column "dependency_name" {
    null = false
    type = character_varying
  }
  column "dependency_id" {
    null = true
    type = uuid
  }
  column "kaskada_view_dependencies" {
    null = false
    type = uuid
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "view_dependencies_kaskada_views_dependencies" {
    columns     = [column.kaskada_view_dependencies]
    ref_columns = [table.kaskada_views.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
}
schema "public" {
}
