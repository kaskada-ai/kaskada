use std::str::FromStr;

use egg::Id;
use smallvec::SmallVec;

use crate::dfg::step_kind::StepKind;

pub(crate) type ChildrenVec = SmallVec<[Id; 2]>;

/// A graph-based representation of the compute plan.
///
/// This corresponds to the Data-Flow graph, where each node is an operator.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct DfgLang {
    /// The kind of node this represents.
    kind: StepKind,

    /// Inputs to the node.
    /// For instructions and sinks this contains the producers of of input
    /// values. For sources that read data this should be empty.
    /// For sources that consume results from earlier passes, this should be the
    /// corresponding sink.
    children: ChildrenVec,
}

// It is weird that we need to implement `Display` for `DfgLang` to pretty print
// only the kind. But, this is a requirement of `egg`.
impl std::fmt::Display for DfgLang {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}

impl DfgLang {
    pub(super) fn new(kind: StepKind, children: ChildrenVec) -> Self {
        Self { kind, children }
    }

    pub fn kind(&self) -> &StepKind {
        &self.kind
    }
}

impl egg::Language for DfgLang {
    fn children(&self) -> &[Id] {
        &self.children
    }

    fn children_mut(&mut self) -> &mut [Id] {
        &mut self.children
    }

    fn matches(&self, other: &Self) -> bool {
        self.kind == other.kind && self.len() == other.len()
    }
}

impl egg::FromOp for DfgLang {
    type Error = anyhow::Error;

    fn from_op(op: &str, children: Vec<Id>) -> Result<Self, Self::Error> {
        let kind = StepKind::from_str(op)?;

        // TODO: Check the number of children.
        // Also, consider using that to shorten the op string?

        let children = SmallVec::from_vec(children);
        Ok(Self { kind, children })
    }
}

#[cfg(test)]
mod tests {
    //! Tests that the language implementation is useable with Egg as expected.
    //! These are *not* meant to be comprehensive tests for the language
    //! representation or any specific rewrite rules.

    use egg::{EGraph, Pattern, Searcher, Var};
    use smallvec::smallvec;
    use sparrow_arrow::scalar_value::ScalarValue;
    use sparrow_instructions::{InstKind, InstOp};

    use super::*;
    use crate::dfg::Expression;

    #[test]
    fn create_interning() {
        let mut egraph: EGraph<DfgLang, ()> = Default::default();
        let five = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(ScalarValue::Int32(Some(5)))),
            smallvec![],
        ));
        let six = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(ScalarValue::Int32(Some(6)))),
            smallvec![],
        ));
        let five_plus_six_a = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::Add))),
            smallvec![five, six],
        ));
        let five_plus_six_b = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::Add))),
            smallvec![five, six],
        ));
        assert_eq!(five_plus_six_a, five_plus_six_b);
    }

    #[test]
    fn create_literal_interning() {
        let mut egraph: EGraph<DfgLang, ()> = Default::default();
        let five_a = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(ScalarValue::Int32(Some(5)))),
            smallvec![],
        ));
        let five_b = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(ScalarValue::Int32(Some(5)))),
            smallvec![],
        ));
        assert_eq!(five_a, five_b);
    }

    #[test]
    fn pattern_matching() {
        // Make a pattern
        let pat: Pattern<DfgLang> = "(add ?x ?y)".parse().unwrap();

        let mut egraph: EGraph<DfgLang, ()> = Default::default();
        let five = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(ScalarValue::Int32(Some(5)))),
            smallvec![],
        ));
        let zero = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(ScalarValue::Int32(Some(0)))),
            smallvec![],
        ));
        let five_plus_zero = egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::Add))),
            smallvec![five, zero],
        ));

        egraph.rebuild();

        let matches = pat.search(&egraph);
        assert_eq!(matches.len(), 1);

        assert_eq!(matches[0].eclass, five_plus_zero);
        assert_eq!(matches[0].substs.len(), 1);
        assert_eq!(
            matches[0].substs[0].get(Var::from_str("?x").unwrap()),
            Some(&five)
        );
        assert_eq!(
            matches[0].substs[0].get(Var::from_str("?y").unwrap()),
            Some(&zero)
        );
    }
}
