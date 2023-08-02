use arrow::datatypes::DataType;
use sparrow_syntax::{FenlType, LiteralValue, Located, ResolvedExpr};

use crate::ast_to_dfg::add_literal;
use crate::dfg::Dfg;
use crate::{AstDfgRef, DataContext, DiagnosticCode, DiagnosticCollector};

/// Flattens window arguments into condition and duration nodes.
///
/// Windows are flattened to components that are executable concepts.
/// Ticks indicate at what times rows will be inserted into tables,
/// and the duration configures when aggregations will reset their
/// internal states.
pub(crate) fn flatten_window_args(
    name: &Located<String>,
    window: &Located<Box<ResolvedExpr>>,
    dfg: &mut Dfg,
    data_context: &mut DataContext,
    diagnostics: &mut DiagnosticCollector<'_>,
) -> anyhow::Result<(Located<AstDfgRef>, Located<AstDfgRef>)> {
    if name.inner() == "since" {
        debug_assert!(
            window.args().len() == 1,
            "expected only one arg for since window, saw {}",
            window.args().len()
        );

        // Since aggregations use a null duration
        let null_arg = dfg.add_literal(LiteralValue::Null.to_scalar()?)?;
        let null_arg = Located::new(
            add_literal(
                dfg,
                null_arg,
                FenlType::Concrete(DataType::Null),
                window.location().clone(),
            )?,
            window.location().clone(),
        );

        let condition = crate::ast_to_dfg(data_context, dfg, diagnostics, &window.args()[0])?;
        Ok((window.with_value(condition), null_arg))
    } else if name.inner() == "sliding" {
        debug_assert!(
            window.args().len() == 2,
            "expected two args for sliding window, saw {}",
            window.args().len()
        );
        let duration = &window.args()[0];
        let duration_node = crate::ast_to_dfg(data_context, dfg, diagnostics, duration)?;
        let duration = duration.with_value(duration_node);

        let condition = crate::ast_to_dfg(data_context, dfg, diagnostics, &window.args()[1])?;
        Ok((window.with_value(condition), duration))
    } else {
        DiagnosticCode::InvalidArgumentType
            .builder()
            .with_label(
                name.location()
                    .primary_label()
                    .with_message(format!("Invalid window function: '{}'", name.inner())),
            )
            .with_note("Supported windows: 'since', 'sliding'".to_string())
            .emit(diagnostics);
        Ok((
            window.with_value(dfg.error_node()),
            window.with_value(dfg.error_node()),
        ))
    }
}
