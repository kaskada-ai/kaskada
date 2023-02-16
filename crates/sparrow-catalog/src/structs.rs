use serde::{Deserialize, Serialize};
use sparrow_api::kaskada::v1alpha::TableConfig;

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct CatalogEntry {
    pub name: String,
    /// A string describing the signature of this function.
    ///
    /// For *most* functions this should be derived from the registered
    /// function. This may need to be provided if the function is not in
    /// the registry due to unsupported signatures (eg., `extend`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_doc: Option<String>,
    /// If set, indicates the entry is for an experimental function.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub long_doc: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub examples: Vec<FunctionExample>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct FunctionExample {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(flatten)]
    pub expression: ExampleExpression,
    /// If set, this is the input CSV to use to create a table with default
    /// options.
    ///
    /// The table will be configured as follows:
    /// - name: "Input"
    /// - time column: "time"
    /// - subsort column: None
    /// - group column: "key"
    /// - grouping: "grouping"
    ///
    /// This means the CSV must contain a `time` and `key` column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_csv: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_csv: Option<String>,
    /// If non-empty, contains explicitly configured tables to use.
    ///
    /// Note: We can't use an enum with `InputCsv` or `Tables` because the
    /// nested table (for tables) must be at the end of the TOML block.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tables: Vec<ExampleTable>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExampleExpression {
    /// Simple examples define a `result` column which is added to the input.
    Expression(String),
    /// More complex examples that filter out rows, or deal with nested records.
    ///
    /// These need complete control over the example expression.
    FullExpression(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct ExampleTable {
    #[serde(flatten)]
    pub table_config: TableConfig,
    pub input_csv: String,
}
