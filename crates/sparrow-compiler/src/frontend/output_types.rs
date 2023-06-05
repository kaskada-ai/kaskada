use arrow::datatypes::{DataType, FieldRef, IntervalUnit, TimeUnit};
use hashbrown::HashSet;
use itertools::izip;
use static_init::dynamic;

use crate::{AstDfgRef, DiagnosticCode, DiagnosticCollector};

// DataTypes that are not encodable to parquet.
//
// https://github.com/apache/parquet-format/blob/master/Encodings.md
#[dynamic]
static UNENCODABLE_TYPES: HashSet<DataType> = {
    let mut m = HashSet::new();

    m.insert(DataType::Interval(IntervalUnit::YearMonth));
    m.insert(DataType::Interval(IntervalUnit::DayTime));

    // MonthDayNano is unsupported in cast.rs
    // m.insert(DataType::Interval(IntervalUnit::MonthDayNano));

    m.insert(DataType::Duration(TimeUnit::Second));
    m.insert(DataType::Duration(TimeUnit::Millisecond));
    m.insert(DataType::Duration(TimeUnit::Microsecond));
    m.insert(DataType::Duration(TimeUnit::Nanosecond));

    m
};

/// Emits diagnostics for unencodable fields.
pub(crate) fn emit_diagnostic_for_unencodable_fields(
    query: &AstDfgRef,
    fields: &[FieldRef],
    diagnostics: &mut DiagnosticCollector<'_>,
) -> anyhow::Result<()> {
    if let Some(fields_ast) = query.fields() {
        anyhow::ensure!(
            fields.len() == fields_ast.len(),
            "Expected fields and asts to match"
        );
        for (field, ast) in izip!(fields, fields_ast) {
            if UNENCODABLE_TYPES.contains(field.data_type()) {
                // TODO: Support specific hints for each type
                DiagnosticCode::InvalidOutputType
                    .builder()
                    .with_label(ast.location().primary_label().with_message(format!(
                        "Output field '{}' has unsupported output type '{}'. Try adding 'as i64'",
                        field.name(),
                        field.data_type()
                    )))
                    .emit(diagnostics);
            }
        }
    } else {
        // TODO: Only some queries contain location information of their fields.
        // Should plumb type explanation through the AST.
        for field in fields {
            if UNENCODABLE_TYPES.contains(field.data_type()) {
                // TODO: Support specific hints for each type
                DiagnosticCode::InvalidOutputType
                    .builder()
                    .with_note(format!(
                        "Output field '{}' has unsupported output type '{}'. Try adding 'as i64'",
                        field.name(),
                        field.data_type()
                    ))
                    .emit(diagnostics);
            }
        }
    }
    Ok(())
}
