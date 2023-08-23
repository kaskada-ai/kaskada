//! Utilities for printing a DFG to DOT (graphviz).

use std::borrow::Cow;

use itertools::Itertools;
use sparrow_instructions::InstKind;

use super::StepKind;
use crate::dfg::{DfgExpr, Expression};

/// Write the DFG for the given expression to `w` in DOT format.
///
/// This uses a subgraph for each operation.
pub(super) fn dfg_expr_to_dot(expr: &DfgExpr, w: &mut impl std::io::Write) -> anyhow::Result<()> {
    writeln!(w, "digraph dfg {{")?;
    writeln!(w, "rankdir = \"BT\";  ")?;

    for (operation, ids) in &expr.ids().group_by(|id| expr.operation(*id)) {
        if let Some(operation) = operation {
            let kind = expr.kind(operation);
            let label: &'static str = match kind {
                StepKind::Operation(op) => op.into(),
                unexpected => anyhow::bail!("Unexpected step kind: {unexpected:?}"),
            };
            writeln!(w, "  subgraph cluster_{operation:?} {{")?;
            writeln!(w, "  label = \"{operation} = {label}\";")?;
            writeln!(w, "  labelloc = \"B\";")?;
        }

        for id in ids {
            let (kind, children) = expr.node(id);

            let label = node_label(kind);
            writeln!(
                w,
                "  node_{id:?} [label = \"{id:?} = {label}({})\"];",
                children
                    .iter()
                    .format_with(", ", |elt, f| f(&format_args!("{elt:?}")))
            )?;
        }

        if let Some(operation) = operation {
            writeln!(w, "  }} // cluster_{operation:?}")?;
        }

        writeln!(w)?;
    }

    for id in expr.ids() {
        let (_, children) = expr.node(id);

        if !children.is_empty() {
            writeln!(
                w,
                "  {{{}}} -> node_{id:?};",
                children
                    .iter()
                    .format_with(";", |elt, f| f(&format_args!("node_{elt:?}")))
            )?;
        }
    }

    writeln!(w, "}}")?;
    w.flush()?;

    Ok(())
}

fn node_label(kind: &StepKind) -> Cow<'static, str> {
    match kind {
        StepKind::Operation(operation) => Cow::Borrowed(operation.into()),
        StepKind::Expression(Expression::Literal(literal)) => {
            // TODO: Escape quotes in strings
            let label = format!("literal:{literal}");
            Cow::Owned(label)
        }
        StepKind::Expression(Expression::LateBound(late_bound)) => {
            Cow::Borrowed(late_bound.label())
        }
        StepKind::Expression(Expression::Inst(InstKind::Simple(inst))) => {
            Cow::Borrowed(inst.into())
        }
        StepKind::Expression(Expression::Inst(InstKind::FieldRef)) => Cow::Borrowed("field_ref"),
        StepKind::Expression(Expression::Inst(InstKind::Cast(to_type))) => {
            let label = format!("cast:{}", sparrow_syntax::FormatDataType(to_type));
            Cow::Owned(label)
        }
        StepKind::Expression(Expression::Inst(InstKind::Record)) => Cow::Borrowed("record"),
        StepKind::Transform => Cow::Borrowed("transform"),
        StepKind::Error => Cow::Borrowed("error"),
        StepKind::Window(window) => Cow::Borrowed(window.label()),
        StepKind::Expression(Expression::Inst(InstKind::Udf(udf))) => {
            Cow::Owned(udf.signature().name().to_owned())
        }
    }
}
