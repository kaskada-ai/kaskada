//! An expression based representation of the DFG.

use anyhow::Context;
use egg::{AstSize, Extractor, Id, Language, RecExpr};

use super::DfgLang;
use crate::dfg::{simplification, DfgGraph, StepKind};
use crate::CompilerOptions;

/// The expression within the DFG.
///
/// The main component is a [RecExpr] of [DfgLang] nodes. This is provided by
/// [egg] and corresponds to a a vector of DFG nodes with inputs appearing
/// before outputs. This allows the inputs to be referenced by [Id] (index) into
/// the vector. It also simplifies many passes over the expression, since they
/// can just iterate over the nodes collecting information in a vector.
pub struct DfgExpr {
    // The RecExpr containing the DFG nodes.
    expr: RecExpr<DfgLang>,
}

impl std::fmt::Debug for DfgExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfgExpr").field("expr", &self.expr).finish()
    }
}

impl DfgExpr {
    pub(super) fn new(expr: RecExpr<DfgLang>) -> Self {
        Self { expr }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            expr: Vec::with_capacity(capacity).into(),
        }
    }

    pub fn len(&self) -> usize {
        self.expr.as_ref().len()
    }

    pub(super) fn add(&mut self, node: DfgLang) -> Id {
        self.expr.add(node)
    }

    pub fn ids(&self) -> impl Iterator<Item = Id> + DoubleEndedIterator {
        // This could return a `Range`, but it would require (currently) unstable
        // functionality.
        (0..self.len()).map(Id::from)
    }

    pub(crate) fn node(&self, id: Id) -> (&StepKind, &[Id]) {
        let expr = &self.expr[id];
        (expr.kind(), expr.children())
    }

    pub(crate) fn kind(&self, id: Id) -> &StepKind {
        self.expr[id].kind()
    }

    #[cfg(test)]
    pub(crate) fn pretty(&self, width: usize) -> String {
        self.expr.pretty(width)
    }

    pub(super) fn expr(&self) -> &RecExpr<DfgLang> {
        &self.expr
    }

    pub fn simplify(self, options: &CompilerOptions) -> anyhow::Result<Self> {
        let _span = tracing::info_span!("Running simplificatons").entered();

        let mut graph = DfgGraph::default();
        let id = graph.add_expr(&self.expr);
        let graph = simplification::run_simplifications(graph, options)?;

        let extractor = Extractor::new(&graph, AstSize);
        let (best_cost, best_expr) = extractor.find_best(id);

        tracing::info!(
            "Extracted expression with cost {} and length {}",
            best_cost,
            best_expr.as_ref().len()
        );

        Ok(Self::new(best_expr))
    }

    pub fn operation(&self, id: Id) -> Option<Id> {
        let node = &self.expr[id];
        match node.kind() {
            StepKind::Operation(_) => Some(id),
            StepKind::Expression(_) | StepKind::Transform => node.children().last().cloned(),
            StepKind::Error => None,
            StepKind::Window(_) => None,
        }
    }

    pub fn write_dot(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let mut file = std::fs::File::create(path).context("create file")?;
        super::dfg_to_dot::dfg_expr_to_dot(self, &mut file)?;

        Ok(())
    }

    pub fn dot_string(&self) -> anyhow::Result<String> {
        let mut bytes: Vec<u8> = Vec::new();
        super::dfg_to_dot::dfg_expr_to_dot(self, &mut bytes)?;
        String::from_utf8(bytes).context("converting dot string")
    }
}
