//! Converts logical expressions to physical plans.
//!
//! The goal is to strike a balance between simplicity of the conversion
//! and simplicity of the initial physical plan. In general, this attempts
//! to minimize the number of physical steps produced at the potential cost
//! of duplicating the physical expressions executed within those steps.
//!
//! To do this, it uses a few strategies:
//!
//! 1. A hash-map from step-kind and inputs to step ID is used to ensure
//!    redundant steps aren't created. Note this is a best effort heuristic:
//!    `(merge a b)` and `(merge b a)` are seen as different steps for the
//!    purposes of this "hashcons" map.
//! 2. Expressions within a step are first built up as a sequence of
//!    expression parts (`PhysicalExpr`) before being added to the a step.
//!    This allows different steps to put the expressions in the right place.
//!    For instance, a `select` step can evaluate the predicate expression
//!    directly.
//!
//! # Improvements
//!
//! The expression heuristic could be improved in a variety of ways:
//! 1. We could count how many times each logical expression was referenced
//!    in different steps, in order to determine whether it was worth
//!    introducing a separate projection.
//! 2. We could estimate the cost of physical expressions, in order to
//!    to determine when it was worth introducing a separate projection.
//!    For example, if the sequence of expressions exceeded a certain
//!    threshold, or contained a UDF, we could introduce a projection.

use arrow_schema::{DataType, Field};
use egg::ENodeOrVar;
use itertools::Itertools;
use smallvec::smallvec;
use sparrow_arrow::scalar_value::ScalarValue;

use crate::exprs::{ExprPattern, ExprVec};
use crate::mutable_plan::MutablePlan;
use crate::Error;

/// Manages the conversion of logical expressions to physical plans.
///
/// This doesn't produce optimized / final execution plans. Instead, it
/// produces a relatively naive physical plan that is then improved by
/// the remaining passes comprising the query compilation.
pub(super) struct LogicalToPhysical {
    plan: MutablePlan,
}

#[derive(Debug)]
struct Reference {
    /// Reference to a specific step.
    ///
    /// This should only be `None` if the expression is a literal. It should
    /// not be `None` if the expression is a literal arising within a specific
    /// step (domain) -- only for cases where the value originally has no
    /// domain.
    step_id: Option<sparrow_physical::StepId>,
    /// Expression corresponding to the result of compilation.
    ///
    /// Will be the "identity" pattern containing only `?input` if this
    /// should just use the result of the step.
    expr: ExprPattern,
}

impl Reference {
    /// Create a reference to the input to the given step.
    fn step_input(step_id: sparrow_physical::StepId) -> error_stack::Result<Self, Error> {
        Ok(Self {
            step_id: Some(step_id),
            expr: ExprPattern::new_input()?,
        })
    }

    fn literal(literal: ScalarValue) -> error_stack::Result<Self, Error> {
        let mut expr = ExprPattern::default();
        let data_type = literal.data_type();
        expr.add_instruction("literal", smallvec![literal], smallvec![], data_type)?;
        Ok(Self {
            step_id: None,
            expr,
        })
    }
}

impl LogicalToPhysical {
    pub(super) fn new() -> Self {
        Self {
            plan: MutablePlan::empty(),
        }
    }

    /// Resolve the arguments to specific expressions in a step.
    ///
    /// This will add a `merge` step, if needed, so that all arguments are available in the
    /// same step.
    fn resolve_args(
        &mut self,
        args: Vec<Reference>,
    ) -> error_stack::Result<(sparrow_physical::StepId, Vec<ExprPattern>), Error> {
        match args
            .iter()
            .flat_map(|reference| reference.step_id)
            .unique()
            .sorted()
            .at_most_one()
        {
            Ok(None) => todo!("handle all-literal arguments"),
            Ok(Some(step)) => {
                // There is only one step, which means all of the arguments are already
                // available in the same place. We can use all of those patterns within
                // a single step to provide the arguments.
                let exprs = args
                    .into_iter()
                    .map(|arg| {
                        // It shouldn't be possible to have an argument refer to the entire result of the
                        // projection step at this point (while we're building the physical plan).
                        debug_assert!(
                            arg.step_id.is_none()
                                || arg.step_id.is_some_and(|arg_step| arg_step == step)
                        );
                        arg.expr
                    })
                    .collect();
                Ok((step, exprs))
            }
            Err(must_merge) => {
                // First, create a merge step.
                //
                // The physical plan we produce includes N-way merges. These are much easier to work
                // with while producing the plan since we don't need to recursively generate binary
                // merges and pull the input through each merge. Additionally, it allows a later pass
                // to identify opportunities for sharing common "sub-merges".
                //
                // Further, we can more efficiently implement a multi-way merge operation in terms
                // of binary merges by choosing the order to perform the merges based on the number
                // of rows.
                let inputs: Vec<_> = must_merge.into_iter().collect();
                let result_fields: arrow_schema::Fields = inputs
                    .iter()
                    .map(|input_step| {
                        self.step_type(*input_step).map(|data_type| {
                            Field::new(format!("step_{}", input_step), data_type, true)
                        })
                    })
                    .try_collect()?;
                let result_type = DataType::Struct(result_fields);

                let merged_step = self.plan.get_or_create_step_id(
                    sparrow_physical::StepKind::Merge,
                    inputs.clone(),
                    ExprVec::empty(),
                    result_type,
                );
                let exprs = args
                    .into_iter()
                    .map(|arg| {
                        // Determine which input to the merge we want.
                        // Start with replacement containing `?input => (fieldref ?input "step_<step_id>)")
                        let mut exprs = ExprPattern::new_input()?;

                        // `new_input` adds `?input` as the first expression in the pattern.
                        let input_id = egg::Id::from(0);

                        let data_type = self.reference_type(&arg)?.clone();

                        if let Some(input_step) = arg.step_id {
                            let merged_input_id = exprs.add_instruction(
                                "fieldref",
                                // Note: that `merge` should produce a record with a
                                // field for each merged step, identified by the
                                // step ID being merged. This may change depending
                                // on (a) optimizations that change steps and
                                // whether it is difficult to update these and (b)
                                // how we choose to implement merge.
                                //
                                // It may be more practical to use the "index of the
                                // step ID in the inputs" which would be more stable
                                // as we change step IDs.
                                smallvec![ScalarValue::Utf8(Some(format!("step_{}", input_step)))],
                                smallvec![input_id],
                                data_type,
                            )?;

                            // Then add the actual expression, replacing the `?input` with the fieldref.
                            let mut subst = egg::Subst::with_capacity(1);
                            subst.insert(*crate::exprs::INPUT_VAR, merged_input_id);
                            exprs.add_pattern(&arg.expr, &subst)?;
                        } else {
                            exprs.add_pattern(&arg.expr, &egg::Subst::default())?;
                        }
                        Ok(exprs)
                    })
                    .collect::<error_stack::Result<_, Error>>()?;
                Ok((merged_step, exprs))
            }
        }
    }

    /// Recursive function visiting the nodes in a logical expression.
    ///
    /// The result is a `Reference` which indicates a specific step containing
    /// the result as well as expressions to apply to that steps output in order
    /// to prdouce the result.
    fn visit(&mut self, node: &sparrow_logical::ExprRef) -> error_stack::Result<Reference, Error> {
        match node.name {
            "literal" => {
                let Some(literal) = node.literal_opt() else {
                    error_stack::bail!(Error::invalid_logical_plan(
                        "expected one literal argument to 'literal'"
                    ))
                };
                let literal = logical_to_physical_literal(literal)?;
                Ok(Reference::literal(literal)?)
            }
            "read" => {
                // A logical scan instruction should have a single literal argument
                // containing the UUID of the table to scan.
                let table_id = *node
                    .literal_args
                    .get(0)
                    .ok_or_else(|| Error::invalid_logical_plan("scan missing table ID"))?
                    .as_uuid()
                    .ok_or_else(|| Error::invalid_logical_plan("scan contains non-UUID"))?;

                let step_id = self.plan.get_or_create_step_id(
                    sparrow_physical::StepKind::Read {
                        source_uuid: table_id,
                    },
                    vec![],
                    ExprVec::empty(),
                    node.result_type.clone(),
                );
                Ok(Reference::step_input(step_id)?)
            }
            "fieldref" => {
                // A field ref contains two arguments. The first is the input record and the second
                // should be a literal corresponding to the field name.
                //
                // Note this doesn't use the literal arguments in logical plans.
                let mut input = self.visit(&node.args[0])?;
                let Some(field) = node.args[1].literal_str_opt() else {
                    error_stack::bail!(Error::invalid_logical_plan(
                        "fieldref field was not a literal"
                    ))
                };
                let field = ScalarValue::Utf8(Some(field.to_owned()));
                let base = input.expr.last_value();
                input.expr.add_instruction(
                    "fieldref",
                    smallvec![field],
                    smallvec![base],
                    node.result_type.clone(),
                )?;
                Ok(input)
            }
            other => {
                let Some(instruction) = sparrow_interfaces::expression::intern_name(other) else {
                    error_stack::bail!(Error::invalid_logical_plan(format!(
                        "unknown instruction '{other}' in logical plan"
                    )));
                };

                let args = if instruction != "record" {
                    node.args
                        .iter()
                        .map(|arg| self.visit(arg))
                        .collect::<Result<_, _>>()?
                } else {
                    // The arguments to the record are 2N -- (name, value) for each field.
                    // However, we've already captured the names in the type. We could
                    // have the creation of logical expressions drop the names, but then
                    // we should move similar handling of literals there as well (eg., fieldrefs).
                    //
                    // TODO: Consider introducing a trait that allows (a) typechecking and (b)
                    // configuring how arguments convert to logical nodes. This would benefit UDFs
                    // as well as provide a place to encapsulate the various special handling.
                    //
                    // If such a trait had both `create_logical` and `logical_to_physical`, then
                    // it would give us a place to see and test that the result of creating the
                    // logical node can be turned into physical nodes, etc.
                    node.args
                        .iter()
                        .skip(1)
                        .step_by(2)
                        .map(|arg| self.visit(arg))
                        .collect::<Result<_, _>>()?
                };
                let (step_id, args) = self.resolve_args(args)?;

                assert_eq!(
                    node.literal_args.len(),
                    0,
                    "literal arguments in logical not yet supported"
                );

                let literal_args = smallvec![];
                let expr = ExprPattern::new_instruction(
                    instruction,
                    literal_args,
                    args,
                    node.result_type.clone(),
                )?;
                Ok(Reference {
                    step_id: Some(step_id),
                    expr,
                })
            }
        }
    }

    pub(super) fn apply(
        mut self,
        root: &sparrow_logical::ExprRef,
    ) -> error_stack::Result<sparrow_physical::Plan, Error> {
        let result = self.visit(root)?;

        // Make sure the resulting step is the last step.
        let result_step_id = result.step_id.ok_or_else(|| {
            Error::internal("result must be associated with a step (non-literal)")
        })?;
        assert!(result_step_id == self.plan.last_step_id());

        debug_assert_eq!(self.reference_type(&result)?, root.result_type);

        let output_id = if result.expr.is_identity() {
            // The result is the output of the step.
            // Nothing to do.

            // We could rely on the fact that the "identity" transform can be pretty
            // trivially instantiated within a projection, but we do this check here
            // for the case of the expression being the result of a non-projection
            // step, since it lets us avoid adding the projection step.
            result_step_id
        } else {
            // Add a projection for the remaining expressions.
            //
            // We could attempt to add them to the step if it is already a projection, but we'd
            // need to deal with mutating the expressions in the projection. For now, we think it
            // is easier to treat steps as immutable once created, and then combine steps in a
            // later optimization pass of the physical plan.
            let input_type = self.plan.step_result_type(result_step_id).clone();
            let result_type = root.result_type.clone();
            self.plan.get_or_create_step_id(
                sparrow_physical::StepKind::Project,
                vec![result_step_id],
                result.expr.instantiate(input_type)?,
                result_type,
            )
        };

        let plan = self.plan.finish(output_id)?;
        Ok(plan)
    }

    fn step_type(&self, step_id: sparrow_physical::StepId) -> error_stack::Result<DataType, Error> {
        Ok(self.plan.step_result_type(step_id).clone())
    }

    fn reference_type(&self, reference: &Reference) -> error_stack::Result<DataType, Error> {
        match reference.expr.last() {
            ENodeOrVar::Var(var) if *var == *crate::exprs::INPUT_VAR => {
                let reference_step_id = reference
                    .step_id
                    .ok_or_else(|| Error::internal("literal expressions don't have `?input`"))?;
                self.step_type(reference_step_id)
            }
            ENodeOrVar::Var(other) => {
                error_stack::bail!(Error::internal(format!(
                    "unrecognized variable {other:?} in argument to merge"
                )))
            }
            ENodeOrVar::ENode(expr) => Ok(expr.result_type.clone()),
        }
    }
}

fn logical_to_physical_literal(
    literal: &sparrow_logical::Literal,
) -> error_stack::Result<ScalarValue, Error> {
    match literal {
        sparrow_logical::Literal::Null => todo!(),
        sparrow_logical::Literal::Bool(_) => todo!(),
        sparrow_logical::Literal::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        sparrow_logical::Literal::Int64(_) => todo!(),
        sparrow_logical::Literal::UInt64(_) => todo!(),
        sparrow_logical::Literal::Float64(_) => todo!(),
        sparrow_logical::Literal::Timedelta {
            seconds: _,
            nanos: _,
        } => todo!(),
        sparrow_logical::Literal::Uuid(_) => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_schema::{DataType, Field};

    #[test]
    fn test_logical_to_physical_arithmetic() {
        sparrow_expressions::ensure_registered();

        let struct_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Int64, false),
                Field::new("y", DataType::Float64, false),
            ]
            .into(),
        );

        let uuid1 = uuid::uuid!("00000000-0000-0000-0000-000000000001");

        let group = sparrow_logical::Grouping::new(0);
        let source1 = Arc::new(sparrow_logical::Expr::new_uuid(
            "read",
            uuid1,
            struct_type.clone(),
            group,
        ));
        let x = Arc::new(sparrow_logical::Expr::new_literal_str("x"));
        let y = Arc::new(sparrow_logical::Expr::new_literal_str("y"));
        let x1 =
            Arc::new(sparrow_logical::Expr::try_new("fieldref", vec![source1.clone(), x]).unwrap());
        let physical_x1 = LogicalToPhysical::new().apply(&x1).unwrap();
        insta::assert_yaml_snapshot!(physical_x1);

        let y1 =
            Arc::new(sparrow_logical::Expr::try_new("fieldref", vec![source1, y.clone()]).unwrap());
        let add_x1_y1 =
            Arc::new(sparrow_logical::Expr::try_new("add", vec![x1.clone(), y1]).unwrap());
        let physical_add_x1_y1 = LogicalToPhysical::new().apply(&add_x1_y1).unwrap();
        insta::assert_yaml_snapshot!(physical_add_x1_y1);

        let uuid2 = uuid::uuid!("00000000-0000-0000-0000-000000000002");
        let source2 = Arc::new(sparrow_logical::Expr::new_uuid(
            "read",
            uuid2,
            struct_type.clone(),
            group,
        ));
        let y2 = Arc::new(sparrow_logical::Expr::try_new("fieldref", vec![source2, y]).unwrap());
        let add_x1_y2 = Arc::new(sparrow_logical::Expr::try_new("add", vec![x1, y2]).unwrap());
        let physical_add_x1_y2 = LogicalToPhysical::new().apply(&add_x1_y2).unwrap();
        insta::assert_yaml_snapshot!(physical_add_x1_y2);
    }
}
