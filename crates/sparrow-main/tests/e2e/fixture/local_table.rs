use std::fs::File;

use futures::StreamExt;
use sparrow_api::kaskada::v1alpha::compute_table::FileSet;
use sparrow_api::kaskada::v1alpha::SourceData;
use sparrow_api::kaskada::v1alpha::{
    source_data, ComputeTable, PreparedFile, TableConfig, TableMetadata,
};
use sparrow_runtime::prepare::prepared_batches;
use sparrow_runtime::stores::ObjectStoreRegistry;
use sparrow_runtime::PreparedMetadata;
use tempfile::NamedTempFile;

pub(crate) struct LocalTestTable {
    config: TableConfig,
    metadata: Option<TableMetadata>,
    /// TODO: Support other file sets?
    prepared_files: Vec<PreparedFile>,
    /// Vector holding the prepared NamedTempFile.
    ///
    /// These will be deleted when the table is dropped after the test.
    retained_files: Vec<NamedTempFile>,
}

impl LocalTestTable {
    pub(super) fn new(config: TableConfig) -> Self {
        Self {
            config,
            metadata: None,
            prepared_files: Vec::new(),
            retained_files: Vec::new(),
        }
    }

    pub(super) fn update_table_metadata(&mut self, metadata: TableMetadata) {
        if let Some(existing) = &mut self.metadata {
            assert_eq!(&existing.schema, &metadata.schema);
            existing.file_count += metadata.file_count;
        } else {
            self.metadata = Some(metadata);
        }
    }

    pub fn clear(&mut self) {
        self.prepared_files.clear();
        self.retained_files.clear();
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn table(&self) -> ComputeTable {
        assert!(
            self.metadata.is_some(),
            "Unable to use table without metadata. Either set it or add a file."
        );

        ComputeTable {
            config: Some(self.config.clone()),
            metadata: self.metadata.clone(),
            file_sets: vec![FileSet {
                slice_plan: None,
                prepared_files: self.prepared_files.clone(),
            }],
        }
    }

    pub async fn add_file_source(
        &mut self,
        raw_file_path: &source_data::Source,
    ) -> anyhow::Result<()> {
        tracing::info!("Adding file source: {:?}", raw_file_path);
        let source_data = SourceData {
            source: Some(raw_file_path.clone()),
        };

        self.add_source(&source_data).await
    }

    pub async fn add_source(&mut self, source_data: &SourceData) -> anyhow::Result<()> {
        let prepared_batches_and_metadata = {
            let mut prepared_batches_and_metadata = Vec::new();

            // Fake prepare the batches and write them to a parquet file..
            //
            // TODO: Simulate the actual interaction with prepare (eg., collect raw files
            // and run prepare in response to analysis).
            let mut iter = prepared_batches(
                &ObjectStoreRegistry::default(),
                source_data,
                &self.config,
                &None,
            )
            .await
            .map_err(|e| e.into_error())?;
            while let Some(prepared_batch) = iter.next().await {
                let (prepared_batch, metadata) = prepared_batch
                    .map_err(|err| anyhow::anyhow!("failed getting batch {:?}", err))?;

                let prepared_file = tempfile::Builder::new()
                    .suffix(".parquet")
                    .tempfile()
                    .unwrap();

                let output_file = File::create(prepared_file.path())?;
                let mut output = parquet::arrow::arrow_writer::ArrowWriter::try_new(
                    output_file,
                    prepared_batch.schema(),
                    Some(
                        // Set the created_by before hashing. This ensures the
                        // hash won't change *just* because the Arrow version changes.
                        parquet::file::properties::WriterProperties::builder()
                            .set_created_by("kaskada e2e tests".to_owned())
                            .build(),
                    ),
                )
                .unwrap();

                output.write(&prepared_batch).unwrap();
                output.close().unwrap();

                let metadata_output_file = tempfile::Builder::new()
                    .suffix(".parquet")
                    .tempfile()
                    .unwrap();

                let output_file = File::create(metadata_output_file.path())?;
                let mut output = parquet::arrow::arrow_writer::ArrowWriter::try_new(
                    output_file,
                    metadata.schema(),
                    None,
                )
                .unwrap();

                output.write(&metadata).unwrap();
                output.close().unwrap();

                let prepared_metadata = PreparedMetadata::try_from_data(
                    format!("file://{}", prepared_file.path().display()),
                    &prepared_batch,
                    format!("file://{}", metadata_output_file.path().display()),
                )?;

                prepared_batches_and_metadata.push((
                    prepared_metadata,
                    prepared_file,
                    metadata_output_file,
                ));
            }

            prepared_batches_and_metadata
        };

        // this is split out into a separate loop to avoid "cannot borrow `*self` as mutable
        // because it is also borrowed as immutable"
        for (prepared_metadata, prepared_file, metadata_output_file) in
            prepared_batches_and_metadata
        {
            self.update_table_metadata(TableMetadata {
                schema: Some(prepared_metadata.table_schema.as_ref().try_into().unwrap()),
                file_count: 1,
            });

            self.prepared_files
                .push(prepared_metadata.try_into().unwrap());

            self.retained_files.push(prepared_file);
            self.retained_files.push(metadata_output_file);
        }

        Ok(())
    }
}
