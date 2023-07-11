use sparrow_api::kaskada::v1alpha::{
    source_data, ComputeTable, SourceData, TableConfig, TableMetadata,
};
use sparrow_syntax::is_valid_ident;

use crate::fixture::LocalTestTable;

/// Fixture for managing the tables used in local e2e tests.
pub(crate) struct DataFixture {
    tables: Vec<LocalTestTable>,
}

impl DataFixture {
    pub fn new() -> Self {
        sparrow_testing::init_test_logging();
        Self { tables: Vec::new() }
    }

    fn table_index(&self, name: &str) -> Option<usize> {
        self.tables.iter().position(|t| t.name() == name)
    }

    pub fn add_table(&mut self, config: TableConfig) -> &mut LocalTestTable {
        // Confirm there isn't a table with the same name.
        assert!(
            is_valid_ident(&config.name),
            "Table name '{}' is not a valid identifier",
            config.name
        );
        assert!(
            self.table_index(&config.name).is_none(),
            "Table with name '{}' already defined in fixture",
            config.name
        );

        self.tables.push(LocalTestTable::new(config));
        self.tables.last_mut().unwrap()
    }

    // Added to support incremental tests (by adding files).
    pub fn table_mut(&mut self, name: &str) -> &mut LocalTestTable {
        let index = self.table_index(name).unwrap();
        &mut self.tables[index]
    }

    /// Adds a table backed by specified files to the test fixture.
    pub async fn with_table_from_files(
        mut self,
        config: TableConfig,
        raw_file_paths: &[&str],
    ) -> Result<Self, crate::EndToEndError> {
        let table = self.add_table(config);
        for raw_file_path in raw_file_paths {
            let data_path = sparrow_testing::testdata_path(raw_file_path);
            let source_data = SourceData::try_from_local(&data_path).unwrap();
            table.add_file_source(&source_data).await?
        }
        Ok(self)
    }

    /// Adds a table backed by the table produced by the `ParquetTableBuilder`.
    pub async fn with_table_from_parquet(
        mut self,
        config: TableConfig,
        table: crate::ParquetTableBuilder,
    ) -> Result<Self, crate::EndToEndError> {
        let temp_file = table.finish();
        let table = self.add_table(config);
        table
            .add_file_source(&source_data::Source::ParquetPath(format!(
                "file:///{}",
                temp_file.path().display()
            )))
            .await
            .unwrap();
        Ok(self)
    }

    pub async fn with_table_from_csv(
        mut self,
        config: TableConfig,
        csv_content: &str,
    ) -> Result<Self, crate::EndToEndError> {
        let table = self.add_table(config);
        table
            .add_file_source(&source_data::Source::CsvData(csv_content.to_owned()))
            .await
            .unwrap();
        Ok(self)
    }

    /// Add a table with specific metadata (and no backing files) to the
    /// fixture.
    pub fn with_table_metadata(mut self, config: TableConfig, metadata: TableMetadata) -> Self {
        let table = self.add_table(config);
        table.update_table_metadata(metadata);
        self
    }

    pub(super) fn tables(&self) -> Vec<ComputeTable> {
        self.tables.iter().map(LocalTestTable::table).collect()
    }
}
