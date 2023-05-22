use std::fs::File;
use std::path::PathBuf;

use arrow::record_batch::RecordBatchReader;
use chrono::NaiveDateTime;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;
use sparrow_api::kaskada::v1alpha::ComputeSnapshot;
use sparrow_api::kaskada::v1alpha::ComputeSnapshotConfig;
use sparrow_api::kaskada::v1alpha::{destination, Destination};
use sparrow_api::kaskada::v1alpha::{
    CompileRequest, ExecuteRequest, FeatureSet, FileType, Formula, ObjectStoreDestination,
    PerEntityBehavior,
};
use sparrow_compiler::InternalCompileOptions;
use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecordHeader;
use sparrow_runtime::s3::S3Helper;

use crate::DataFixture;

/// Fixture for constructing and executing a query used in local e2e tests.
#[derive(Clone)]
pub(crate) struct QueryFixture {
    compile_request: CompileRequest,
    execute_request: ExecuteRequest,
    internal_compile_options: InternalCompileOptions,
}

#[derive(Debug)]
pub(crate) struct RunResult<T> {
    pub inner: T,
    pub snapshots: Vec<ComputeSnapshot>,
}

impl QueryFixture {
    pub fn new(query: &str) -> Self {
        let compile_request = CompileRequest {
            feature_set: Some(FeatureSet {
                formulas: vec![],
                query: query.to_owned(),
            }),
            expression_kind: ExpressionKind::Complete as i32,
            per_entity_behavior: PerEntityBehavior::All as i32,
            ..CompileRequest::default()
        };

        let execute_request = ExecuteRequest {
            limits: Some(Limits::default()),
            ..ExecuteRequest::default()
        };

        Self {
            compile_request,
            execute_request,
            internal_compile_options: InternalCompileOptions::default(),
        }
    }

    // This can be used to enable rocksdb in a tempdir, but does not
    // upload snapshots to s3. In the normal incremental query path,
    // the presence of a `ComputeSnapshotConfig` determines whether a
    // `storage_path` is created and rocksdb is used.
    pub fn with_rocksdb(
        mut self,
        snapshot_prefix: &std::path::Path,
        resume_from: Option<&std::path::Path>,
    ) -> Self {
        let resume_from = resume_from.map(|snapshot_dir| {
            let snapshot_dir = snapshot_dir.strip_prefix(snapshot_prefix).unwrap();
            snapshot_dir.to_string_lossy().into_owned()
        });

        self.execute_request.compute_snapshot_config = Some(ComputeSnapshotConfig {
            output_prefix: snapshot_prefix.to_string_lossy().into_owned(),
            resume_from,
        });
        self
    }

    pub fn with_final_results(mut self) -> Self {
        self.compile_request.per_entity_behavior = PerEntityBehavior::Final as i32;
        self
    }

    pub fn with_final_results_at_time(mut self, result_time: NaiveDateTime) -> Self {
        self.compile_request.per_entity_behavior = PerEntityBehavior::FinalAtTime as i32;
        self.execute_request.final_result_time = Some(result_time.into());
        self
    }

    pub fn with_changed_since(mut self, changed_since: NaiveDateTime) -> Self {
        self.execute_request.changed_since = Some(changed_since.into());
        self
    }

    pub fn with_formula(mut self, name: &str, formula: &str) -> Self {
        self.compile_request
            .feature_set
            .as_mut()
            .unwrap()
            .formulas
            .push(Formula {
                name: name.to_owned(),
                formula: formula.to_owned(),
                source_location: "test case".to_owned(),
            });
        self
    }

    #[allow(unused)]
    pub fn with_formulas<'a>(
        mut self,
        formulas: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        self.compile_request.feature_set.as_mut().unwrap().formulas = formulas
            .into_iter()
            .map(|(name, formula)| Formula {
                name: name.to_owned(),
                formula: formula.to_owned(),
                source_location: "test case".to_owned(),
            })
            .collect();
        self
    }

    pub fn with_preview_rows(mut self, rows: i64) -> Self {
        self.execute_request.limits.as_mut().unwrap().preview_rows = rows;
        self
    }

    /// Modify the compile options to disable simplification.
    pub fn without_simplification(mut self) -> Self {
        self.internal_compile_options.simplifier_iteration_limit = 0;
        self
    }

    /// Modify the compile options to dump the DFG and Plan dot files, and the
    /// Plan YAML.
    ///
    /// They will be placed in
    /// `<crate>/tests/test-output/<name>_{dfg,plan}.{dot,yaml}`.
    #[allow(dead_code)]
    pub fn with_dump_dot(mut self, name: &str) -> Self {
        let mut test_output_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        test_output_dir.push("tests");
        test_output_dir.push("test-output");

        if !test_output_dir.exists() {
            std::fs::create_dir(&test_output_dir).unwrap()
        }

        self.internal_compile_options = InternalCompileOptions {
            store_initial_dfg: Some(test_output_dir.join(format!("{name}_initial_dfg.dot"))),
            store_final_dfg: Some(test_output_dir.join(format!("{name}_final_dfg.dot"))),
            store_plan_graph: Some(test_output_dir.join(format!("{name}_plan.dot"))),
            store_plan_yaml: Some(test_output_dir.join(format!("{name}_plan.yaml"))),
            ..self.internal_compile_options
        };
        self
    }

    /// Run a query and return the results as a CSV string along with a snapshot
    /// path.
    pub async fn run_snapshot_to_csv(
        &self,
        data: &DataFixture,
    ) -> Result<RunResult<String>, crate::EndToEndError> {
        let output_dir = tempfile::TempDir::new().unwrap();
        let result = self.run(data, FileType::Csv, output_dir.path()).await?;
        let output_file = result
            .inner
            .into_iter()
            .exactly_one()
            .expect("multiple output file not yet supported");
        let output_file = output_file.to_string_lossy().to_string();
        let output_file = output_file.strip_prefix("file://").expect("file:// prefix");

        Ok(RunResult {
            inner: std::fs::read_to_string(output_file).unwrap(),
            snapshots: result.snapshots,
        })
    }

    /// Run a query and return the results as a CSV string.
    pub async fn run_to_csv(&self, data: &DataFixture) -> Result<String, crate::EndToEndError> {
        let output_dir = tempfile::TempDir::new().unwrap();
        let result = self.run(data, FileType::Csv, output_dir.path()).await?;
        let output_file = result
            .inner
            .into_iter()
            .exactly_one()
            .expect("multiple output file not yet supported");
        let output_file = output_file.to_string_lossy().to_string();
        let output_file = output_file.strip_prefix("file://").expect("file:// prefix");

        Ok(std::fs::read_to_string(output_file).unwrap())
    }

    /// Run a query writing to a temporary Parquet file, and return it.
    pub async fn run_to_parquet(
        &self,
        data: &DataFixture,
    ) -> Result<PathBuf, crate::EndToEndError> {
        let output_dir = tempfile::TempDir::new().unwrap();
        let result = self.run(data, FileType::Parquet, output_dir.path()).await?;
        let output_file = result
            .inner
            .into_iter()
            .exactly_one()
            .expect("multiple output file not yet supported");
        let output_file = output_file.to_string_lossy().to_string();
        let output_file = output_file.strip_prefix("file://").expect("file:// prefix");
        let mut output_path = PathBuf::new();
        output_path.push(output_file);

        Ok(output_path)
    }

    /// Run a query and return the hash of the resulting Parquet file.
    pub async fn run_to_parquet_hash(
        &self,
        data: &DataFixture,
    ) -> Result<String, crate::EndToEndError> {
        let output_dir = tempfile::TempDir::new().unwrap();
        let result = self.run(data, FileType::Parquet, output_dir.path()).await?;
        let output_file = result
            .inner
            .into_iter()
            .exactly_one()
            .expect("multiple output file not yet supported");
        let output_file = output_file.to_string_lossy().to_string();
        let output_file = output_file.strip_prefix("file://").expect("file:// prefix");
        let mut output_path = PathBuf::new();
        output_path.push(output_file);

        Ok(hash_parquet_file(&output_path))
    }

    /// Run a query to the given output location.
    pub async fn run(
        &self,
        data: &DataFixture,
        output_format: FileType,
        output_dir: &std::path::Path,
    ) -> Result<RunResult<Vec<PathBuf>>, crate::EndToEndError> {
        let request = CompileRequest {
            tables: data.tables(),
            ..self.compile_request.clone()
        };

        // TODO: Look at caching / reusing the compiled plan.
        let compile_result =
            sparrow_compiler::compile_proto(request, self.internal_compile_options.clone()).await?;

        let plan = if let Some(plan) = compile_result.plan {
            plan
        } else {
            return Err(compile_result.fenl_diagnostics.unwrap_or_default().into());
        };

        // TODO: If this is expensive to construct each time in tests (it shouldn't be)
        // we could put it in a `[dynamic]` static, and clone it. Or we could have a
        // special "for test" version.
        let s3_helper = S3Helper::new().await;

        let destination = ObjectStoreDestination {
            output_prefix_uri: format!("file:///{}", output_dir.display()),
            file_type: output_format.into(),
            output_paths: None,
        };
        let output_to = Destination {
            destination: Some(destination::Destination::ObjectStore(destination)),
        };

        let request = ExecuteRequest {
            plan: Some(plan),
            destination: Some(output_to),
            tables: data.tables(),

            ..self.execute_request.clone()
        };

        let mut stream = sparrow_runtime::execute::execute(
            request,
            s3_helper,
            None,
            None,
            FlightRecordHeader::default(),
        )
        .await?
        .boxed();

        let mut output_files = Vec::new();
        let mut snapshots = Vec::new();
        while let Some(next) = stream.try_next().await? {
            if let Some(output_paths) = next.output_paths() {
                output_files.extend(output_paths.into_iter().map(PathBuf::from));
            }

            snapshots.extend(next.compute_snapshots);
        }

        output_files.sort();
        output_files.dedup();

        Ok(RunResult {
            inner: output_files,
            snapshots,
        })
    }
}

/// Return the hash of a parquet file as an uppercase hex string.
fn hash_parquet_file(file: &std::path::Path) -> String {
    // Read the file, concatenate all the batches, and write it back.
    // This is curretly necessary since different batch divisions may lead to
    // different hashes, even if the actual content is identical.
    // TODO: Figure out why the batching in the output is nondeterministic.

    let concat_file = tempfile::Builder::new()
        .prefix("concat")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    {
        let file = File::open(file).unwrap();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(1000);
        let reader = parquet_reader.build().unwrap();
        let schema = reader.schema();
        let batches: Vec<_> = reader.try_collect().unwrap();
        let concatenated = arrow::compute::concat_batches(&schema, &batches).unwrap();

        let concat_file = concat_file.reopen().unwrap();
        let mut writer = ArrowWriter::try_new(
            concat_file,
            schema,
            Some(
                // Set the created_by before hashing. This ensures the
                // hash won't change *just* because the Arrow version changes.
                WriterProperties::builder()
                    .set_created_by("kaskada e2e tests".to_owned())
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&concatenated).unwrap();
        writer.close().unwrap();
    }

    // Sha2 sum the concatenated file.
    use sha2::Digest;
    let mut hasher = sha2::Sha224::new();
    let mut input_file = concat_file.into_file();
    std::io::copy(&mut input_file, &mut hasher).unwrap();
    let hash = hasher.finalize();

    data_encoding::HEXUPPER.encode(&hash)
}
