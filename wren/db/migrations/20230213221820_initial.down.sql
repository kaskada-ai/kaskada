-- reverse: create "prepare_job_kaskada_files" table
DROP TABLE "prepare_job_kaskada_files";
-- reverse: create "view_dependencies" table
DROP TABLE "view_dependencies";
-- reverse: create "prepared_files" table
DROP TABLE "prepared_files";
-- reverse: create index "kaskadaview_name_owner_kaskada_views" to table: "kaskada_views"
DROP INDEX "kaskadaview_name_owner_kaskada_views";
-- reverse: create "kaskada_views" table
DROP TABLE "kaskada_views";
-- reverse: create "data_tokens" table
DROP TABLE "data_tokens";
-- reverse: create "kaskada_table_compute_snapshots" table
DROP TABLE "kaskada_table_compute_snapshots";
-- reverse: create "compute_snapshots" table
DROP TABLE "compute_snapshots";
-- reverse: create "materialization_dependencies" table
DROP TABLE "materialization_dependencies";
-- reverse: create "prepare_jobs" table
DROP TABLE "prepare_jobs";
-- reverse: create index "materialization_name_owner_materializations" to table: "materializations"
DROP INDEX "materialization_name_owner_materializations";
-- reverse: create "materializations" table
DROP TABLE "materializations";
-- reverse: create "kaskada_query_results" table
DROP TABLE "kaskada_query_results";
-- reverse: create "kaskada_queries" table
DROP TABLE "kaskada_queries";
-- reverse: create index "kaskadafile_identifier_kaskada_table_kaskada_files" to table: "kaskada_files"
DROP INDEX "kaskadafile_identifier_kaskada_table_kaskada_files";
-- reverse: create "kaskada_files" table
DROP TABLE "kaskada_files";
-- reverse: create "data_versions" table
DROP TABLE "data_versions";
-- reverse: create index "kaskadatable_name_owner_kaskada_tables" to table: "kaskada_tables"
DROP INDEX "kaskadatable_name_owner_kaskada_tables";
-- reverse: create "kaskada_tables" table
DROP TABLE "kaskada_tables";
-- reverse: create "owners" table
DROP TABLE "owners";
