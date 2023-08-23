//! Implement the DFG analyses for the E-graph library.
//!
//! [`DfgAnalysis`] implements the [`Analysis`] trait which instructs
//! the E-graph library how to create [`DfgAnalysisData`] for each node
//! added to the DFG and merge information between nodes when they are
//! determined to be equivalent.
use egg::{Analysis, Id, Language};
use hashbrown::HashMap;
use itertools::Itertools;
use smallvec::smallvec;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::{InstKind, InstOp};

use super::{DfgGraph, DfgLang, StepKind};
use crate::dfg::Expression;

/// The E-graph analysis used to create and merge [`DfgAnalysisData`].
#[derive(Default, Debug)]
pub(crate) struct DfgAnalysis;

/// Analysis data that is added to each DFG node.
///
/// This is computed from the DFG nodes, and propagated along the nodes
/// as equivalent nodes are discovered, etc.
///
/// Some information (such as shape and type) is a little less intuitive
/// here than if it were specified while constructing the node. However,
/// having to specify it when creating the node would make simplification
/// more difficult, since every simplification rule would need to specify
/// the new shape/type information.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DfgAnalysisData {
    /// The node defining the operation (domain) this node is in.
    domain: Domain,
    /// The literal value equivalent to this node, if any.
    literal: Option<ScalarValue>,
    /// If this node has known fields.
    known_fields: Option<HashMap<Id, Id>>,
    /// The type of node
    kind: StepKind,
    /// Children to the node
    children: Vec<Id>,
}

#[derive(Debug, Clone, PartialEq)]
enum Domain {
    /// The DFG node is an operation, and is thus its own operation.
    /// This needs to be a separate analysis, since the node hasn't been
    /// created (assigned an ID) yet.
    Operation,
    /// The DFG node is an expression associated with the given operation ID.
    Expression(Id),
}

impl DfgAnalysisData {
    pub(crate) fn literal_opt(&self) -> Option<&ScalarValue> {
        self.literal.as_ref()
    }

    /// Return the operation of this node.
    ///
    /// It must be passed it's own ID since that wasn't available when
    /// the node was created, and for operation nodes, the ID of the
    /// operation is the ID of the node.
    pub(crate) fn operation(&self, self_id: Id) -> Id {
        match self.domain {
            Domain::Operation => self_id,
            Domain::Expression(id) => id,
        }
    }

    pub(crate) fn kind(&self) -> StepKind {
        self.kind.clone()
    }
}

impl Analysis<DfgLang> for DfgAnalysis {
    type Data = DfgAnalysisData;

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> egg::DidMerge {
        let mut a_modified = false;
        let mut b_modified = false;

        // Merge literal
        match (a.literal_opt(), b.literal_opt()) {
            (None, Some(literal)) => {
                a.literal = Some(literal.clone());
                a_modified = true;
            }
            (Some(_), None) => {
                b_modified = true;
            }
            (None, None) => {
                // Nothing to change
            }
            (Some(lit_a), Some(lit_b)) if lit_a.is_null() && lit_b.is_null() => {
                // This is weird -- they may be *different scalars* but both
                // null. In theory, we shouldn't be merging them, but it could
                // happen.
            }
            (Some(lit_a), Some(lit_b)) => {
                assert_eq!(lit_a, lit_b, "Literals must be equal when merging.")
            }
        }

        egg::DidMerge(a_modified, b_modified)
    }

    fn make(egraph: &DfgGraph, enode: &DfgLang) -> Self::Data {
        // If this is a record creation, we have known field values.
        let known_fields =
            if let StepKind::Expression(Expression::Inst(InstKind::Record)) = enode.kind() {
                Some(enode.children().iter().copied().tuples().collect())
            } else {
                None
            };

        let literal = match enode.kind() {
            StepKind::Expression(Expression::Literal(value)) => Some(value.clone()),
            // If all of the arguments to an instruction (omitting the operation) are constant,
            // then the expression can be constant evaluated.
            StepKind::Expression(Expression::Inst(inst))
                if enode.children()[0..enode.children().len() - 1]
                    .iter()
                    .all(|arg| egraph[*arg].data.literal_opt().is_some()) =>
            {
                // If all the inputs are literals, just evaluate the node to get a literal.
                let literals: Vec<_> = enode.children()[0..enode.children().len() - 1]
                    .iter()
                    .map(|id| egraph[*id].data.literal_opt().unwrap().clone())
                    .collect();
                let literal = super::const_eval::evaluate_constant(inst, literals)
                    .expect("Result of constant evaluation");
                Some(literal)
            }
            StepKind::Transform => egraph[enode.children()[0]].data.literal_opt().cloned(),
            _ => None,
        };

        let domain = match enode.kind() {
            StepKind::Operation(_) => Domain::Operation,
            StepKind::Expression(_) | StepKind::Transform => {
                let operation = enode.children().last().unwrap_or_else(|| {
                    panic!(
                        "expressions must have at least one argument but {:?} did not",
                        enode.kind(),
                    )
                });
                Domain::Expression(*operation)
            }
            StepKind::Error => Domain::Operation,
            StepKind::Window(_) => Domain::Operation,
        };

        let children: Vec<Id> = enode.children().to_vec();

        DfgAnalysisData {
            domain,
            literal,
            known_fields,
            kind: enode.kind().clone(),
            children,
        }
    }

    fn modify(egraph: &mut DfgGraph, id: Id) {
        let data = &egraph[id].data;

        let operation = data.operation(id);
        let mut to_merge = Vec::new();

        let is_literal = if let Some(value) = data.literal_opt().cloned() {
            to_merge.push(EquivalentTo::Literal(value));
            true
        } else {
            false
        };

        for node in egraph[id].nodes.iter() {
            if !is_literal
                && matches!(node.kind(), StepKind::Expression(Expression::Inst(_)))
                && node
                    .children()
                    .iter()
                    .all(|arg| egraph[*arg].data.literal_opt().is_some())
            {
                if let StepKind::Expression(Expression::Inst(inst)) = node.kind() {
                    // If all the inputs are literals, just evaluate the node to get a literal.
                    let literals: Vec<_> = node
                        .children()
                        .iter()
                        .map(|id| egraph[*id].data.literal_opt().unwrap().clone())
                        .collect();
                    let literal = super::const_eval::evaluate_constant(inst, literals)
                        .expect("Result of constant evaluation");
                    to_merge.push(EquivalentTo::Literal(literal));
                    break;
                }
            } else if let StepKind::Expression(Expression::Inst(InstKind::FieldRef)) = node.kind() {
                // For each first field ref with `known_fields` equivalent to this `id`,
                // Union this `id` and the corresponding known field.

                let base = node.children()[0];
                let field = node.children()[1];

                // If the base has known fields, we can dereference them.
                if let Some(base_known_fields) = &egraph[base].data.known_fields {
                    let field_value = base_known_fields.get(&field).unwrap_or_else(|| {
                        panic!("Missing field value for {field} in {base_known_fields:?}")
                    });
                    to_merge.push(EquivalentTo::Node(*field_value));
                }

                // If the base is a `null_if` or an `if` we can push the field-ref inside.
                //
                // - `null_if(cond, base).field = null_if(cond, base.field)`
                // - `if(cond, base).field = if(cond, base.field)`.
                //
                // We can't (currently) express these easily using field refs since we can't
                // match on the field name.
                for base_node in egraph[base].nodes.iter() {
                    match base_node.kind() {
                        StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::If))) => {
                            to_merge.push(EquivalentTo::If {
                                cond: base_node.children()[0],
                                base: base_node.children()[1],
                                field,
                            });
                        }
                        StepKind::Expression(Expression::Inst(InstKind::Simple(
                            InstOp::NullIf,
                        ))) => {
                            to_merge.push(EquivalentTo::NullIf {
                                cond: base_node.children()[0],
                                base: base_node.children()[1],
                                field,
                            });
                        }
                        _ => (),
                    }
                }
            }
        }

        for equivalent in to_merge {
            match equivalent {
                EquivalentTo::Literal(literal) => {
                    let literal = egraph.add(DfgLang::new(
                        StepKind::Expression(Expression::Literal(literal)),
                        smallvec![operation],
                    ));
                    egraph.union(id, literal);
                }
                EquivalentTo::Node(equivalent) => {
                    egraph.union(id, equivalent);
                }
                EquivalentTo::NullIf { cond, base, field } => {
                    let cond = into_operation(egraph, cond, operation);
                    let base = into_operation(egraph, base, operation);
                    let field = into_operation(egraph, field, operation);
                    let base_field = egraph.add(DfgLang::new(
                        StepKind::Expression(Expression::Inst(InstKind::FieldRef)),
                        smallvec![base, field, operation],
                    ));
                    let equivalent = egraph.add(DfgLang::new(
                        StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::NullIf))),
                        smallvec![cond, base_field, operation],
                    ));
                    egraph.union(id, equivalent);
                }
                EquivalentTo::If { cond, base, field } => {
                    let cond = into_operation(egraph, cond, operation);
                    let base = into_operation(egraph, base, operation);
                    let field = into_operation(egraph, field, operation);

                    let base_field = egraph.add(DfgLang::new(
                        StepKind::Expression(Expression::Inst(InstKind::FieldRef)),
                        smallvec![base, field, operation],
                    ));
                    let equivalent = egraph.add(DfgLang::new(
                        StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::If))),
                        smallvec![cond, base_field, operation],
                    ));
                    egraph.union(id, equivalent);
                }
            }
        }
    }
}

fn into_operation(egraph: &mut DfgGraph, desired: Id, operation: Id) -> Id {
    let node = &egraph[desired];
    if node.data.operation(desired) == operation {
        desired
    } else if let Some(literal) = node.data.literal_opt().cloned() {
        egraph.add(DfgLang::new(
            StepKind::Expression(Expression::Literal(literal)),
            smallvec![operation],
        ))
    } else {
        egraph.add(DfgLang::new(
            StepKind::Transform,
            smallvec![desired, operation],
        ))
    }
}

enum EquivalentTo {
    Literal(ScalarValue),
    Node(Id),
    NullIf { cond: Id, base: Id, field: Id },
    If { cond: Id, base: Id, field: Id },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_same() {
        // Merging is used when two nodes are identified as equal. We test it directly
        // here.
        let mut a = DfgAnalysisData {
            domain: Domain::Operation,
            literal: None,
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };
        let b = DfgAnalysisData {
            domain: Domain::Operation,
            literal: None,
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };

        let mut analysis = DfgAnalysis;
        let merge_result = analysis.merge(&mut a, b);
        assert!(!merge_result.0);
        assert!(!merge_result.1);
        assert_eq!(
            a,
            DfgAnalysisData {
                domain: Domain::Operation,
                literal: None,
                known_fields: None,
                kind: StepKind::Error,
                children: Vec::new(),
            }
        );
    }

    #[test]
    fn merge_lhs_literal() {
        // Merging is used when two nodes are identified as equal. We test it directly
        // here.
        let mut a = DfgAnalysisData {
            domain: Domain::Operation,
            literal: Some(ScalarValue::Int64(Some(5))),
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };
        let b = DfgAnalysisData {
            domain: Domain::Operation,
            literal: None,
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };

        let mut analysis = DfgAnalysis;
        let merge_result = analysis.merge(&mut a, b);
        assert!(!merge_result.0);
        assert!(merge_result.1);
        assert_eq!(
            a,
            DfgAnalysisData {
                domain: Domain::Operation,
                literal: Some(ScalarValue::Int64(Some(5))),
                known_fields: None,
                kind: StepKind::Error,
                children: Vec::new(),
            }
        );
    }

    #[test]
    fn merge_rhs_literal() {
        // Merging is used when two nodes are identified as equal. We test it directly
        // here.
        let mut a = DfgAnalysisData {
            domain: Domain::Operation,
            literal: None,
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };
        let b = DfgAnalysisData {
            domain: Domain::Operation,
            literal: Some(ScalarValue::Int64(Some(5))),
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };

        let mut analysis = DfgAnalysis;
        let merge_result = analysis.merge(&mut a, b);
        assert!(merge_result.0);
        assert!(!merge_result.1);
        assert_eq!(
            a,
            DfgAnalysisData {
                domain: Domain::Operation,
                literal: Some(ScalarValue::Int64(Some(5))),
                known_fields: None,
                kind: StepKind::Error,
                children: Vec::new(),
            }
        );
    }

    #[test]
    #[should_panic(expected = "Literals must be equal when merging")]
    pub fn test_merge_incompatible_literal() {
        // Merging is used when two nodes are identified as equal. We test it directly
        // here.
        let mut a = DfgAnalysisData {
            domain: Domain::Operation,
            literal: Some(ScalarValue::Int64(Some(5))),
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };
        let b = DfgAnalysisData {
            domain: Domain::Operation,
            literal: Some(ScalarValue::Int64(Some(6))),
            known_fields: None,
            kind: StepKind::Error,
            children: Vec::new(),
        };

        let mut analysis = DfgAnalysis;
        let merge_result = analysis.merge(&mut a, b);
        assert!(merge_result.0);
        assert!(!merge_result.1);
    }
}
