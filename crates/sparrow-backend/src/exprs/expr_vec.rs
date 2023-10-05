use index_vec::IndexVec;
use sparrow_physical::ExprId;

use crate::exprs::expr_lang::ExprLang;
use crate::Error;

/// An linear representation of the expressions.
///
/// Unlike `ExprGraph` (which is built on `egg::EGraph`) this does not
/// deduplicate the expressions internally. However, when added to a `ExprGraph`
/// or `EGraph` deduplication will be performed.
#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub(crate) struct ExprVec {
    pub(super) expr: egg::RecExpr<ExprLang>,
}

impl ExprVec {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.expr.as_ref().len()
    }

    pub fn to_physical_exprs(&self) -> error_stack::Result<sparrow_physical::Exprs, Error> {
        let mut exprs = IndexVec::with_capacity(self.len());
        for expr in self.expr.as_ref() {
            // let expr = self.graph[expr].clone();
            let args = expr
                .args
                .iter()
                .map(|arg| {
                    let id: usize = (*arg).into();
                    ExprId::from(id)
                })
                .collect();

            exprs.push(sparrow_physical::Expr {
                name: std::borrow::Cow::Borrowed(expr.name),
                literal_args: expr.literal_args.iter().cloned().collect(),
                args,
                result_type: expr.result_type.clone(),
            });
        }
        Ok(exprs)
    }
}
