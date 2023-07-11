use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use tempfile::NamedTempFile;

const PARQUET_FILE_TIME: filetime::FileTime = filetime::FileTime::from_unix_time(100_042, 42);

/// Utility for creating a Parquet file from Arrow arrays.
pub(crate) struct ParquetTableBuilder {
    fields: Vec<Field>,
    columns: Vec<Arc<dyn Array>>,
}

impl ParquetTableBuilder {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn add_column(mut self, name: &str, nullable: bool, values: impl Array + 'static) -> Self {
        if !nullable {
            assert_eq!(
                values.null_count(),
                0,
                "Non-nullable column '{}' contained {} nulls",
                name,
                values.null_count()
            );
        }

        if let Some(first_column) = self.columns.first() {
            assert_eq!(
                first_column.len(),
                values.len(),
                "Expected all columns to have same length ({}), but '{}' had length {}",
                first_column.len(),
                name,
                values.len()
            );
        }

        self.fields
            .push(Field::new(name, values.data_type().clone(), nullable));
        self.columns.push(Arc::new(values));
        self
    }

    /// Write the parquet file to the given `NamedTempFile`.
    ///
    /// Note: The file will be deleted when the `NamedTempFile` is dropped.
    pub fn finish(self) -> NamedTempFile {
        // Create the schema and batch.
        let schema = Arc::new(Schema::new(self.fields));
        let batch = RecordBatch::try_new(schema, self.columns).unwrap();

        // Write the data to the file.
        let temp_file = tempfile::Builder::new()
            .prefix("test_file")
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        {
            let mut parquet_writer = parquet::arrow::ArrowWriter::try_new(
                temp_file.reopen().unwrap(),
                batch.schema(),
                Some(
                    // Set the created_by before hashing. This ensures the
                    // hash won't change *just* because the Arrow version changes.
                    parquet::file::properties::WriterProperties::builder()
                        .set_created_by("kaskada e2e tests".to_owned())
                        .build(),
                ),
            )
            .unwrap();
            parquet_writer.write(&batch).unwrap();
            parquet_writer.close().unwrap();
        }

        // The file modification time is used as the prepare hash for local files.
        // For determinism, we set this to a fixed value in tests.
        filetime::set_file_mtime(temp_file.path(), PARQUET_FILE_TIME)
            .expect("set modification time");

        temp_file
    }
}
