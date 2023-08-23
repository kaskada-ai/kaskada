use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::{DataType, SchemaRef};
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use sparrow_api::kaskada::v1alpha::{compute_table, ComputeTable, PreparedFile, TableConfig};
use sparrow_core::context_code;
use sparrow_instructions::{GroupId, TableId};
use sparrow_merge::InMemoryBatches;
use sparrow_syntax::Location;
use uuid::Uuid;

use crate::dfg::{Dfg, Operation};
use crate::AstDfgRef;

/// Represents the "data context" for a compilation.
///
/// Specifically, this holds the information about the tables
/// available to the compilation.
#[derive(Default, Debug, Clone)]
pub struct DataContext {
    /// Information about the groupings in the context.
    group_info: Vec<GroupInfo>,

    /// Information about each of the tables in the context.
    table_info: BTreeMap<TableId, TableInfo>,
}

impl DataContext {
    /// Create a DataContext from the given files.
    pub fn try_from_tables(tables: Vec<ComputeTable>) -> anyhow::Result<Self> {
        let mut data_context = DataContext::default();
        for table in tables {
            data_context.add_table(table)?;
        }
        Ok(data_context)
    }

    /// Retrieve the `GroupId` of the grouping with the given name.
    pub fn group_id(&self, grouping: &str) -> Option<GroupId> {
        self.group_info
            .iter()
            .position(|p| p.name == grouping)
            .map(GroupId::from)
    }

    /// Retrieve the `TableId` of the table with the given name.
    pub fn table_id(&self, table_name: &str) -> Option<TableId> {
        self.table_info
            .iter()
            .find(|(_, v)| v.name() == table_name)
            .map(|(k, _)| *k)
    }

    pub fn group_info(&self, id: GroupId) -> Option<&GroupInfo> {
        self.group_info.get(id.index())
    }

    pub fn table_info(&self, id: TableId) -> Option<&TableInfo> {
        self.table_info.get(&id)
    }

    pub fn tables_for_grouping(&self, id: GroupId) -> impl Iterator<Item = &TableInfo> {
        self.table_info
            .iter()
            .filter(move |(_, table_info)| table_info.group_id() == id)
            .map(|(_, table_info)| table_info)
    }

    pub fn get_or_create_group_id(
        &mut self,
        grouping_name: &str,
        key_type: &DataType,
    ) -> anyhow::Result<GroupId> {
        if let Some(group_id) = self.group_id(grouping_name) {
            let group_info = self
                .group_info(group_id)
                .context("missing group info for id")?;
            anyhow::ensure!(
                key_type == &group_info.key_type,
                context_code!(
                    tonic::Code::InvalidArgument,
                    "Group '{}' expects key type {:?} but was {:?}",
                    grouping_name,
                    group_info.key_type,
                    key_type
                )
            );
            Ok(group_id)
        } else {
            let group_id = GroupId::from(self.group_info.len());
            let group_info = GroupInfo {
                name: grouping_name.to_owned(),
                key_type: key_type.clone(),
            };
            self.group_info.push(group_info);
            Ok(group_id)
        }
    }

    /// Add a table to the data context.
    pub fn add_table(&mut self, table: ComputeTable) -> anyhow::Result<&mut TableInfo> {
        let config = table
            .config
            .as_ref()
            // Without the table config we don't have a name to report.
            .with_context(|| context_code!(tonic::Code::InvalidArgument, "Table missing config"))?;

        if self.table_id(&config.name).is_some() {
            anyhow::bail!(context_code!(
                tonic::Code::InvalidArgument,
                "Table '{}' defined multiple times",
                config.name
            ));
        }

        // 1. Get the schema from the table (metadata or source).
        let schema: SchemaRef = if let Some(table_metadata) = &table.metadata {
            let schema = table_metadata.schema.as_ref().with_context(|| {
                context_code!(
                    tonic::Code::InvalidArgument,
                    "Table '{}' metadata missing schema",
                    config.name
                )
            })?;
            let schema = schema.as_arrow_schema().with_context(|| {
                context_code!(
                    tonic::Code::InvalidArgument,
                    "Table '{}' has invalid schema",
                    config.name
                )
            })?;
            Arc::new(schema)
        } else {
            anyhow::bail!(context_code!(
                tonic::Code::InvalidArgument,
                "Table '{}' missing metadata",
                config.name
            ));
        };

        // 2. Get the key type from the table config and schema.
        let key_type = schema
            .field_with_name(&config.group_column_name)
            .with_context(|| {
                context_code!(
                    tonic::Code::InvalidArgument,
                    "Grouping column name '{}' not defined in table '{}'",
                    config.group_column_name,
                    config.name
                )
            })?
            .data_type();

        // 3. Get (or create) the group ID for the table grouping.
        let grouping_name = if config.grouping.is_empty() {
            &config.name
        } else {
            &config.grouping
        };
        let group_id = self.get_or_create_group_id(grouping_name, key_type)?;

        // 4. Create the table info and add to the set.
        let table_uuid = Uuid::parse_str(&config.uuid).context("parsing string to table uuid")?;
        let table_id = TableId::new(table_uuid.to_owned());
        let table_info = TableInfo::try_new(table_id, group_id, schema, table)?;
        let table_info = self.table_info.entry(table_id).or_insert(table_info);
        Ok(table_info)
    }

    pub fn table_infos(&self) -> impl Iterator<Item = &'_ TableInfo> {
        self.table_info.values()
    }

    /// Creates a DFG with nodes for the corresponding tables.
    pub(crate) fn create_dfg(&self) -> anyhow::Result<Dfg> {
        let mut dfg = Dfg::default();
        // Add the tables from the context to the DFG.
        for table_info in self.table_info.values() {
            let dfg_node = table_info.dfg_node(&mut dfg)?;
            dfg.bind(table_info.name(), dfg_node);
        }
        Ok(dfg)
    }

    /// Create a DataContext with a few tables for testing purposes.
    ///
    /// Creates a DataContext with the following tables:
    ///
    /// - `Table1` with the type: `{ x_i64: i64, y_i64: i64, s_str: string,
    ///   z_i32: i32, f_f64: f64, a_bool: bool, b_bool: bool }`
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        use sparrow_api::kaskada::v1alpha::Schema;
        use sparrow_api::kaskada::v1alpha::TableMetadata;
        let mut data_context = DataContext::default();

        use arrow::datatypes::Field;
        let schema1 = arrow::datatypes::Schema::new(vec![
            Field::new("x_i64", DataType::Int64, true),
            Field::new("y_i64", DataType::Int64, true),
            Field::new("s_str", DataType::Utf8, true),
            Field::new("z_i32", DataType::Int32, true),
            Field::new("f_f64", DataType::Float64, true),
            Field::new("a_bool", DataType::Boolean, true),
            Field::new("b_bool", DataType::Boolean, true),
        ]);
        let schema1: Schema = Schema::try_from(&schema1).expect("Invalid schema for test");

        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "Table1",
                    &Uuid::from_u64_pair(0, 0),
                    "time",
                    Some("x_i64"),
                    "x_i64",
                    "group",
                )),
                metadata: Some(TableMetadata {
                    schema: Some(schema1),
                    file_count: 10,
                }),
                file_sets: vec![],
            })
            .unwrap();

        let schema2 = arrow::datatypes::Schema::new(vec![
            Field::new("x_i64", DataType::Int64, true),
            Field::new("y_i64", DataType::Int64, true),
        ]);
        let schema2: Schema = Schema::try_from(&schema2).expect("Invalid schema for test");

        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "Table2",
                    &Uuid::from_u64_pair(0, 1),
                    "time",
                    Some("x_i64"),
                    "x_i64",
                    "group",
                )),
                metadata: Some(TableMetadata {
                    schema: Some(schema2),
                    file_count: 10,
                }),
                file_sets: vec![],
            })
            .unwrap();

        let schema3 = arrow::datatypes::Schema::new(vec![
            Field::new("x_i64", DataType::Int64, true),
            Field::new("y_i64", DataType::Int64, true),
        ]);
        let schema3: Schema = Schema::try_from(&schema3).expect("Invalid schema for test");
        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig::new_with_table_source(
                    "Table3",
                    &Uuid::from_u64_pair(0, 2),
                    "time",
                    Some("x_i64"),
                    "x_i64",
                    "group",
                )),
                metadata: Some(TableMetadata {
                    schema: Some(schema3),
                    file_count: 10,
                }),
                file_sets: vec![],
            })
            .unwrap();

        data_context
    }

    #[cfg(test)]
    pub(crate) fn proto_tables(&self) -> anyhow::Result<Vec<ComputeTable>> {
        use itertools::Itertools;
        use sparrow_api::kaskada::v1alpha::TableMetadata;

        Itertools::try_collect(self.table_infos().map(|info| -> anyhow::Result<_> {
            Ok(ComputeTable {
                config: Some(info.config().as_ref().clone()),
                metadata: Some(TableMetadata {
                    schema: Some(info.schema().as_ref().try_into()?),
                    file_count: 0,
                }),
                // This is currently only used for compilation tests that
                // ignore the filesets.
                file_sets: vec![],
            })
        }))
    }
}

/// Information about groups.
#[derive(Debug, Clone)]
pub struct GroupInfo {
    name: String,
    key_type: DataType,
}

/// Information about tables.
#[derive(Debug, Clone)]
pub struct TableInfo {
    table_id: TableId,
    group_id: GroupId,
    schema: SchemaRef,
    config: Arc<TableConfig>,
    /// The file sets representing the table.
    ///
    /// Each file set corresponds to the files for the table with a specific
    /// slice configuration.
    file_sets: Vec<compute_table::FileSet>,
    /// An in-memory record batch for the contents of the table.
    pub in_memory: Option<Arc<InMemoryBatches>>,
}

impl TableInfo {
    fn try_new(
        table_id: TableId,
        group_id: GroupId,
        schema: SchemaRef,
        table: ComputeTable,
    ) -> anyhow::Result<Self> {
        let ComputeTable {
            config, file_sets, ..
        } = table;

        let config = Arc::new(config.context("missing table config")?);

        Ok(Self {
            table_id,
            group_id,
            schema,
            config,
            file_sets,
            in_memory: None,
        })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn uuid(&self) -> &str {
        &self.config.uuid
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn config(&self) -> &Arc<TableConfig> {
        &self.config
    }

    pub fn file_sets(&self) -> &[compute_table::FileSet] {
        &self.file_sets
    }

    pub fn prepared_files_for_slice(
        &self,
        requested_slice: &Option<Slice>,
    ) -> anyhow::Result<&[PreparedFile]> {
        let file_set = self
            .file_sets
            .iter()
            .find(|set| {
                set.slice_plan
                    .iter()
                    .all(|slice| &slice.slice == requested_slice)
            })
            .with_context(|| {
                context_code!(
                    tonic::Code::InvalidArgument,
                    "Table '{}' missing file set with requested slice {:?}",
                    self.name(),
                    requested_slice
                )
            })?;

        Ok(&file_set.prepared_files)
    }

    pub fn metadata_for_files(&self) -> Vec<String> {
        self.file_sets
            .iter()
            .flat_map(|set| {
                set.prepared_files
                    .iter()
                    .map(|file| file.metadata_path.clone())
            })
            .collect()
    }

    pub fn dfg_node(&self, dfg: &mut Dfg) -> anyhow::Result<AstDfgRef> {
        use smallvec::smallvec;
        use sparrow_instructions::InstOp;
        use sparrow_syntax::FenlType;

        use crate::ast_to_dfg::AstDfg;
        use crate::time_domain::TimeDomain;

        // Add the table reference to the environment.
        let value = dfg.add_operation(
            Operation::Scan {
                table_id: self.table_id,
                slice: None,
            },
            smallvec![],
        )?;
        let is_new = dfg.add_instruction(InstOp::IsValid, smallvec![value])?;

        let value_type = DataType::Struct(self.schema().fields().clone());
        let value_type = FenlType::Concrete(value_type);

        Ok(Arc::new(AstDfg::new(
            value,
            is_new,
            value_type,
            Some(self.group_id()),
            TimeDomain::table(self.table_id),
            // TODO: Include a [FeatureSetPart] for internal nodes.
            Location::internal_str("table_definition"),
            None,
        )))
    }
}

impl GroupInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_type(&self) -> &DataType {
        &self.key_type
    }
}
