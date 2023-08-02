use anyhow::Context;

use crate::dfg::language::DfgLang;
use crate::dfg::{DfgExpr, StepKind};

/// Rewrites a `DfgExpr` to remove "useless transforms".
///
/// A transform is useless if it transforms a value into the
/// operation it is already in.
///
/// We have simplification rules which treat these as equivalent,
/// however simplification doesn't always saturation so it is not
/// guaranteed that all such nodes are removed. This pass ensures
/// that any later analysis of the DFG does not have to deal with
/// useless transforms.
pub fn remove_useless_transforms(expr: DfgExpr) -> anyhow::Result<DfgExpr> {
    let mut rewritten = DfgExpr::with_capacity(expr.len());
    let mut rewritten_ids = Vec::with_capacity(expr.len());

    for id in expr.ids() {
        let (kind, args) = expr.node(id);

        let rewritten_args: smallvec::SmallVec<[_; 2]> = args
            .iter()
            .map(|old_id| rewritten_ids[usize::from(*old_id)])
            .collect();

        let rewritten_id = match kind {
            StepKind::Transform => {
                let value_id = rewritten_args[0];
                let dst_operation = rewritten_args[1];

                let (value_kind, value_args) = rewritten.node(value_id);
                let src_operation = match value_kind {
                    StepKind::Operation(_) | StepKind::Error => value_id,
                    StepKind::Transform | StepKind::Expression(_) => value_args
                        .last()
                        .copied()
                        .context("transforms and expressions must have at least one argument")?,
                    StepKind::Window(_) => todo!(),
                };

                if dst_operation == src_operation {
                    value_id
                } else {
                    rewritten.add(DfgLang::new(StepKind::Transform, rewritten_args))
                }
            }
            _ => rewritten.add(DfgLang::new(kind.clone(), rewritten_args)),
        };

        rewritten_ids.push(rewritten_id);
    }

    Ok(rewritten)
}
