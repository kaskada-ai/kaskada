#![allow(clippy::large_enum_variant)] // Could box the pulsar subscription, but for now just allowing this.
#![allow(clippy::derive_partial_eq_without_eq)]
use std::str::FromStr; // stop clippy erroring on generated code from tonic (proto)

use itertools::Itertools;

use crate::kaskada::v1alpha::object_store_destination::ResultPaths;
use crate::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;

tonic::include_proto!("kaskada.kaskada.v1alpha");

/// Traits for the `Slice`
mod slice_traits;

/// Implementations and traits for parts of the plan
mod plan_impl;

/// Traits for [PreparedFile]
mod prepared_file_impl;

/// Traits for [Schema]
mod schema_traits;

impl ComputeTable {
    pub fn name(&self) -> &str {
        &self.config().name
    }

    pub fn config(&self) -> &TableConfig {
        self.config.as_ref().expect("tables should have a config")
    }
}

impl ExecuteResponse {
    /// Returns an owned vec of the output paths for this execution.
    //
    /// Returns `None` if not an `ObjectStoreOutput`.
    pub fn output_paths(&self) -> Option<Vec<String>> {
        match &self.destination {
            Some(output) => match &output.destination {
                Some(destination::Destination::ObjectStore(store)) => store
                    .output_paths
                    .as_ref()
                    .map(|output_paths| output_paths.paths.clone()),
                Some(_) | None => None,
            },
            None => None,
        }
    }

    /// Returns a mutable reference to the output paths.
    ///
    /// Returns `None` if not an `ObjectStoreOutput`.
    pub fn output_paths_mut(&mut self) -> Option<&mut ResultPaths> {
        match &mut self.destination {
            Some(output) => match &mut output.destination {
                Some(destination::Destination::ObjectStore(store)) => match &mut store.output_paths
                {
                    Some(output_path) => Some(output_path),
                    None => None,
                },
                Some(_) | None => None,
            },
            None => None,
        }
    }
}

impl FeatureSet {
    pub fn new(query: &str, formulas: Vec<(&str, &str)>) -> Self {
        Self {
            formulas: formulas
                .into_iter()
                .enumerate()
                .map(|(index, (name, formula))| Formula {
                    name: name.to_owned(),
                    formula: formula.to_owned(),
                    source_location: format!("FeatureSet::new formula {index}"),
                })
                .collect(),
            query: query.to_owned(),
        }
    }
}

impl TableConfig {
    /// Creates a table config with the given parameters and a `KaskadaSource`,
    /// which represents a file-backed table.
    pub fn new_with_table_source(
        name: &str,
        uuid: &uuid::Uuid,
        time_column_name: &str,
        subsort_column_name: Option<&str>,
        group_column_name: &str,
        grouping: &str,
    ) -> Self {
        Self {
            name: name.to_owned(),
            uuid: uuid.to_string(),
            time_column_name: time_column_name.to_owned(),
            subsort_column_name: subsort_column_name.map(|s| s.to_owned()),
            group_column_name: group_column_name.to_owned(),
            grouping: grouping.to_owned(),
            source: Some(Source {
                source: Some(source::Source::Kaskada(KaskadaSource {})),
            }),
        }
    }

    pub fn new2(
        name: &str,
        time_column_name: &str,
        subsort_column_name: Option<&str>,
        group_column_name: &str,
        grouping: &str,
    ) -> Self {
        Self::new_with_table_source(
            name,
            &uuid::Uuid::new_v4(),
            time_column_name,
            subsort_column_name,
            group_column_name,
            grouping,
        )
    }

    pub fn with_table_source(self) -> Self {
        Self::new_with_table_source(
            &self.name,
            &uuid::Uuid::from_str(&self.uuid).unwrap(),
            &self.time_column_name,
            self.subsort_column_name.as_deref(),
            &self.group_column_name,
            &self.grouping,
        )
    }
}

impl Formula {
    pub fn new(name: &str, formula: &str) -> Self {
        Self {
            name: name.to_owned(),
            formula: formula.to_owned(),
            source_location: name.to_owned(),
        }
    }
}

// Display PlanHash as the upper hex encoded vector.
impl std::fmt::Display for PlanHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.hash
                .iter()
                .format_with("", |elt, f| f(&format_args!("{elt:X}")))
        )
    }
}

impl SourceData {
    pub fn try_from_local(path: &std::path::Path) -> anyhow::Result<source_data::Source> {
        let path = match path.extension().and_then(|ext| ext.to_str()) {
            Some("parquet") => {
                source_data::Source::ParquetPath(format!("file://{}", path.display()))
            }
            Some("csv") => source_data::Source::CsvPath(format!("file://{}", path.display())),
            unsupported => anyhow::bail!("Unsupported extension {:?}", unsupported),
        };
        Ok(path)
    }
}

impl std::fmt::Display for FenlDiagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            Itertools::intersperse(
                self.fenl_diagnostics
                    .iter()
                    .map(|d| -> &str { &d.formatted }),
                ""
            )
            .flat_map(|f| f.lines())
            .map(|s| s.trim_end())
            .format("\n")
        )
    }
}

impl std::fmt::Display for TickBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TickBehavior::Minutely => write!(f, "minutely"),
            TickBehavior::Hourly => write!(f, "hourly"),
            TickBehavior::Daily => write!(f, "daily"),
            TickBehavior::Monthly => write!(f, "monthly"),
            TickBehavior::Yearly => write!(f, "yearly"),
            TickBehavior::Finished => write!(f, "final"),
            TickBehavior::Unspecified => panic!("Unspecified tick behavior"),
        }
    }
}

impl From<uuid::Uuid> for Uuid {
    fn from(uuid: uuid::Uuid) -> Self {
        let (high, low) = uuid.as_u64_pair();
        Self { high, low }
    }
}

impl From<&uuid::Uuid> for Uuid {
    fn from(uuid: &uuid::Uuid) -> Self {
        let (high, low) = uuid.as_u64_pair();
        Self { high, low }
    }
}

impl From<Uuid> for uuid::Uuid {
    fn from(uuid: Uuid) -> Self {
        Self::from_u64_pair(uuid.high, uuid.low)
    }
}

impl From<&Uuid> for uuid::Uuid {
    fn from(uuid: &Uuid) -> Self {
        Self::from_u64_pair(uuid.high, uuid.low)
    }
}
