//! Conversion from the Fenl  AST to DFG nodes.

mod ast_dfg;
mod record_ops_to_dfg;
mod window_args;

#[cfg(test)]
mod tests;
use std::rc::Rc;

use anyhow::{anyhow, Context};
use arrow::datatypes::{DataType, FieldRef};
pub use ast_dfg::*;
use egg::Id;
use itertools::{izip, Itertools};
use record_ops_to_dfg::*;
use smallvec::{smallvec, SmallVec};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::CastEvaluator;
use sparrow_plan::{GroupId, InstKind, InstOp};
use sparrow_syntax::{
    ExprOp, FenlType, FormatDataType, LiteralValue, Located, Location, Resolved, ResolvedExpr,
};

use self::window_args::flatten_window_args;
use crate::dfg::{Dfg, Expression, Operation};
use crate::diagnostics::DiagnosticCode;
use crate::time_domain::TimeDomain;
use crate::types::inference::instantiate;
use crate::{DataContext, DiagnosticBuilder, DiagnosticCollector};

/// Convert the `expr` to corresponding DFG nodes.
pub(super) fn ast_to_dfg(
    data_context: &mut DataContext,
    dfg: &mut Dfg,
    diagnostics: &mut DiagnosticCollector<'_>,
    expr: &ResolvedExpr,
) -> anyhow::Result<AstDfgRef> {
    if matches!(expr.op(), ExprOp::Error) {
        // This happens if we're asked to create the DFG for an expression that
        // failed to parse. Generally, we should have reported the entire expression
        // as a diagnostic error, so we just make sure that the result is an error node.
        return Ok(dfg.error_node());
    }

    let arguments = expr.args();

    // Create the DFG for each argument. This is usually straightforward, unless
    // the operator has bind values. In that case, we need to handle the environment
    // specially.
    let arguments = match expr.op() {
        ExprOp::Pipe(_) => {
            let lhs = ast_to_dfg(data_context, dfg, diagnostics, &arguments[0])?;
            dfg.enter_env();
            dfg.bind("$input", lhs);
            let rhs = ast_to_dfg(data_context, dfg, diagnostics, &arguments[1])?;
            dfg.exit_env();
            return Ok(rhs);
        }
        ExprOp::Let(names, _) => {
            dfg.enter_env();
            // The last name (and argument) correspond to the `let_body`.
            let bindings = names.len() - 1;
            // Skip the last argument (which will correspond to the let body).
            for (name, value) in izip!(names, arguments.values()).take(bindings) {
                let value = ast_to_dfg(data_context, dfg, diagnostics, value)?;
                dfg.bind(name.inner(), value);
            }
            let result = ast_to_dfg(
                data_context,
                dfg,
                diagnostics,
                &arguments.values()[names.len() - 1],
            )?;
            dfg.exit_env();
            return Ok(result);
        }
        // Note: Now that `AstDfgRef` contains a `Location`, this likely does not
        // need to be wrapped in `Located`.
        _ => arguments.try_transform(|e| -> anyhow::Result<Located<AstDfgRef>> {
            let ast_dfg = ast_to_dfg(data_context, dfg, diagnostics, e.inner())?;
            Ok(e.with_value(ast_dfg))
        })?,
    };

    add_to_dfg(
        data_context,
        dfg,
        diagnostics,
        expr.op(),
        arguments,
        Some(expr),
    )
}

pub(super) fn add_to_dfg(
    data_context: &mut DataContext,
    dfg: &mut Dfg,
    diagnostics: &mut DiagnosticCollector<'_>,
    op: &ExprOp,
    mut arguments: Resolved<Located<AstDfgRef>>,
    expr: Option<&ResolvedExpr>,
) -> anyhow::Result<AstDfgRef> {
    let argument_types = arguments.transform(|i| i.with_value(i.value_type().clone()));

    match op {
        ExprOp::Literal(literal) => {
            let (value_id, value_type) = match literal.inner() {
                LiteralValue::String(s) => {
                    let id = dfg.add_string_literal(s)?;
                    (id, DataType::Utf8)
                }
                not_string => {
                    let value = not_string.to_scalar()?;
                    let data_type = value.data_type();
                    let id = dfg.add_literal(value)?;
                    (id, data_type)
                }
            };
            Ok(add_literal(
                dfg,
                value_id,
                value_type.into(),
                literal.location().clone(),
            )?)
        }
        ExprOp::Cast(to_type, _) => {
            let input = &arguments[0];
            let from_type = &argument_types[0];

            if from_type.is_error() {
                // Avoid returning an error when cast "<invalid_expr> as i64".
                return Ok(dfg.error_node());
            }

            if CastEvaluator::is_supported_fenl(from_type, to_type) {
                if let FenlType::Concrete(to_type) = to_type.inner() {
                    return Ok(Rc::new(AstDfg::new(
                        dfg.add_expression(
                            Expression::Inst(InstKind::Cast(to_type.clone())),
                            smallvec![input.value()],
                        )?,
                        input.is_new(),
                        to_type.clone().into(),
                        input.grouping(),
                        input.time_domain().clone(),
                        input.location().clone(),
                        None,
                    )));
                }
            }

            // If we get here we have failed to cast.
            DiagnosticCode::IllegalCast
                .builder()
                .with_label(
                    to_type
                        .location()
                        .primary_label()
                        .with_message(format!("Unable to cast to type '{}'", to_type.inner())),
                )
                .with_note(format!("From type {from_type}"))
                .emit(diagnostics);
            Ok(dfg.error_node())
        }
        ExprOp::Reference(reference) => match dfg.get_binding(reference) {
            Ok(value) => Ok(value),
            Err(nearest) => {
                DiagnosticCode::UnboundReference
                    .builder()
                    .with_label(
                        reference
                            .location()
                            .primary_label()
                            .with_message(format!("No reference named '{reference}'")),
                    )
                    .with_note(if nearest.is_empty() {
                        "No formulas, tables, or let-bound names available".to_owned()
                    } else {
                        format!(
                            "Nearest matches: {}",
                            nearest
                                .iter()
                                .format_with(", ", |e, f| f(&format_args!("'{e}'")))
                        )
                    })
                    .emit(diagnostics);
                Ok(dfg.error_node())
            }
        },
        ExprOp::FieldRef(field, _) => {
            let base = &arguments[0];
            let base_type = &argument_types[0];

            let field_type = match base_type.inner() {
                FenlType::Concrete(DataType::Struct(fields)) => {
                    if let Some(field) = fields.iter().find(|f| f.name() == field.inner()) {
                        field.data_type()
                    } else {
                        missing_field_diagnostic(fields, field.inner(), field.location())
                            .emit(diagnostics);
                        return Ok(dfg.error_node());
                    }
                }
                FenlType::Json => {
                    // This is a pseudo-hack that allows us to support json datatypes without
                    // a specific arrow-representable json type. All `json` functions are converted
                    // to `json_field` instructions that take a `string` and output a `string`,
                    // hence the `utf8` return type here.
                    &DataType::Utf8
                }
                FenlType::Error => {
                    // The original error is already reported.
                    return Ok(dfg.error_node());
                }
                _ => {
                    DiagnosticCode::IllegalFieldRef
                        .builder()
                        .with_label(
                            // If the base is not a struct, that is the "primary" problem.
                            arguments[0]
                                .location()
                                .primary_label()
                                .with_message(format!(
                                    "No fields for non-record base type {base_type}"
                                )),
                        )
                        .emit(diagnostics);
                    return Ok(dfg.error_node());
                }
            };

            let field_name = dfg.add_string_literal(field.inner())?;
            let value = dfg.add_expression(
                Expression::Inst(InstKind::FieldRef),
                smallvec![base.value(), field_name],
            )?;
            let is_new = base.is_new();
            let value_type = field_type.clone().into();
            Ok(Rc::new(AstDfg::new(
                value,
                is_new,
                value_type,
                base.grouping(),
                base.time_domain().clone(),
                base.location().clone(),
                None,
            )))
        }
        ExprOp::Call(function_name) => {
            // Assumption: All instructions are exposed as Fenl functions. It seems
            // reasonable / desirable to keep this consistency, until such
            // time as it is clear they should diverge.

            let function = crate::functions::get_function(function_name).map_err(|candidates| {
                // This is an internal error, because the problem should have been reported
                // when resolving arguments.
                anyhow!(
                    "Missing function {}: candidates {:?}",
                    function_name,
                    candidates
                )
            })?;

            if function.is_tick() {
                // This is a strange pattern - when creating the initial tick argument, we don't
                // yet know the input. However, we ensure that ticks are recreated with the
                // correct input given the context of their environment.
                //
                // TODO: Can we move this before we create the args, so we don't have to
                // recreate them?
                let behavior = function.tick_behavior().context("tick behavior")?;
                if let Ok(agg_input) = dfg.get_binding("$condition_input") {
                    // The argument is a tick, so we can directly create the necessary node.
                    let agg_input_op = dfg.operation(agg_input.value());
                    let tick_input = smallvec![agg_input_op];
                    let tick_node = dfg.add_operation(Operation::Tick(behavior), tick_input)?;
                    let tick_node = Rc::new(AstDfg::new(
                        tick_node,
                        tick_node,
                        FenlType::Concrete(DataType::Boolean),
                        agg_input.grouping(),
                        agg_input.time_domain().clone(),
                        function_name.location().clone(),
                        None,
                    ));
                    return Ok(tick_node);
                }
            }

            let mut invalid = false;
            for constant_index in function.signature().parameters().constant_indices() {
                let argument = &arguments.values()[constant_index];
                if dfg.literal(argument.value()).is_none() {
                    invalid = true;

                    let argument_name = &function.signature().arg_names()[constant_index];
                    DiagnosticCode::InvalidNonConstArgument
                        .builder()
                        .with_label(argument.location().primary_label().with_message(format!(
                            "Argument '{argument_name}' to '{function_name}' must be constant, but was not"
                        )))
                        .emit(diagnostics)
                }
            }

            if invalid {
                return Ok(dfg.error_node());
            }

            let (instantiated_types, instantiated_result_type) =
                match instantiate(function_name, &argument_types, function.signature()) {
                    Ok(result) => result,
                    Err(diagnostic) => {
                        diagnostic.emit(diagnostics);
                        return Ok(dfg.error_node());
                    }
                };

            // If any arguments were an error, bail out, returning the error node.
            // This may prevent us from recovering from an error in certain cases, but
            // we currently deem that difficult to maintain. For instance, we'd need
            // to invent the proper grouping post-recovery, which would require error
            // nodes with grouping information.
            if argument_types.iter().any(|arg| arg.is_error()) {
                return Ok(dfg.error_node());
            }

            // TODO: Drive grouping determination from the function itself.
            let grouping = match function.name() {
                "lookup" => {
                    let foreign_group_id = if let Some(grouping) = arguments[1].grouping() {
                        grouping
                    } else {
                        DiagnosticCode::InvalidArguments
                            .builder()
                            .with_label(
                                arguments[1]
                                    .location()
                                    .primary_label()
                                    .with_message("Invalid un-grouped foreign value for lookup."),
                            )
                            .emit(diagnostics);
                        return Ok(dfg.error_node());
                    };

                    // Make sure the key data type is the same as (or convertible) to the
                    // foreign key type. To do this, we update the instantiated type to be
                    // it needs to be, and rely on the existing type error rules.
                    let actual_key_type = instantiated_types
                        .get("key")
                        .context("lookup missing key")?;

                    let foreign_group_info = data_context
                        .group_info(foreign_group_id)
                        .context("no grouping for lookup")?;

                    let expected_key_type = foreign_group_info.key_type();

                    match actual_key_type {
                        FenlType::Error => return Ok(dfg.error_node()),
                        FenlType::Concrete(actual_key_type)
                            if actual_key_type == expected_key_type =>
                        {
                            // This case is OK
                        }
                        _ => {
                            // TODO: We could allow key types that may be
                            // implicitly cast by updating the instantiated
                            // types. But this seems like a good start:
                            // Return an error if the key type does not
                            // match the foreign key.

                            let key_argument_location = argument_types
                                .get("key")
                                .context("lookup missing key")?
                                .location();
                            let value_argument_location = argument_types
                                .get("value")
                                .context("lookup missing value")?
                                .location();
                            DiagnosticCode::InvalidArgumentType
                                .builder()
                                .with_label(
                                    key_argument_location
                                        .primary_label()
                                        .with_message(format!("Actual key type {actual_key_type}")),
                                )
                                .with_label(value_argument_location.secondary_label().with_message(
                                    format!(
                                        "Grouping '{}' expects key type {}",
                                        foreign_group_info.name(),
                                        FormatDataType(foreign_group_info.key_type())
                                    ),
                                ))
                                .emit(diagnostics);

                            return Ok(dfg.error_node());
                        }
                    }

                    // Don't bother checking the groupings -- the only requirements are:
                    // 1. That the value has a grouping (necessary to confirm the key matches
                    // it). This is checked above.
                    //
                    // 2. The result has the same grouping as the key.
                    if let Some(grouping) = arguments[0].grouping() {
                        Some(grouping)
                    } else {
                        DiagnosticCode::InvalidArguments
                            .builder()
                            .with_label(
                                arguments[0]
                                    .location()
                                    .primary_label()
                                    .with_message("Invalid un-grouped foreign key for lookup."),
                            )
                            .emit(diagnostics);
                        return Ok(dfg.error_node());
                    }
                }
                "with_key" => {
                    // Check the arguments have the same partitioning.
                    verify_same_partitioning(
                        data_context,
                        diagnostics,
                        &function_name.with_value(function_name.inner()),
                        &arguments,
                    )?;

                    // But the output grouping depends on the specified grouping name.
                    let grouping = arguments
                        .get("grouping")
                        .context("with_key arguments missing 'grouping'")?;
                    let grouping_name = dfg
                        .literal(grouping.value())
                        .context("with_key 'grouping' was not literal")?;
                    let key_type = arguments[0].value_type();
                    let key_type = match key_type {
                        FenlType::Concrete(concrete) => concrete,
                        FenlType::Error => {
                            // Propagate the error since it is already reported.
                            return Ok(dfg.error_node());
                        }
                        invalid => {
                            DiagnosticCode::InvalidArgumentType
                                .builder()
                                .with_label(
                                    arguments[0]
                                        .location()
                                        .primary_label()
                                        .with_message(format!("Invalid key type '{invalid}'")),
                                )
                                .emit(diagnostics);
                            return Ok(dfg.error_node());
                        }
                    };

                    let group_id = match grouping_name {
                        ScalarValue::Utf8(Some(grouping_name)) => {
                            data_context.get_or_create_group_id(grouping_name, key_type)?
                        }
                        ScalarValue::Null | ScalarValue::Utf8(None) => {
                            let grouping_name = FormatDataType(key_type).to_string();
                            let group_id =
                                data_context.get_or_create_group_id(&grouping_name, key_type)?;

                            // Update the arguments to contain the computed grouping name.
                            //
                            // This is a bit hacky, but the scheduler doesn't
                            // have type information so it can't compute the
                            // grouping name. Ideally, this could be part of
                            // "special" logic associated with the `with_key`
                            // function, but we don't currently have those
                            // extension points.
                            let grouping_name = dfg.add_string_literal(&grouping_name)?;
                            let grouping_name = add_literal(
                                dfg,
                                grouping_name,
                                DataType::Utf8.into(),
                                grouping.location().clone(),
                            )?;

                            arguments.values_mut()[2].update_value(grouping_name);
                            group_id
                        }
                        unsupported => anyhow::bail!(
                            "Grouping name should be string, but was {:?}",
                            unsupported
                        ),
                    };
                    Some(group_id)
                }
                // TODO: Determine the grouping for regrouped results.
                // We probably *still* want to verify that the `key` and `value` arguments
                // are compatible grouped, but we may want to allow specifying the grouping
                // of the result. In fact, that may be required? Specifically, to use this
                // with other tables grouped by the same entity (as well as to schedule it
                // correctly) we'll likely need to have a group ID.
                _ => verify_same_partitioning(
                    data_context,
                    diagnostics,
                    &function_name.with_value(function_name.inner()),
                    &arguments,
                )?,
            };

            // Add cast operations as necessary
            let args: Vec<_> = izip!(arguments, instantiated_types)
                .map(|(arg, expected_type)| -> anyhow::Result<_> {
                    let ast_dfg = Rc::new(AstDfg::new(
                        cast_if_needed(dfg, arg.value(), arg.value_type(), &expected_type)?,
                        arg.is_new(),
                        expected_type,
                        arg.grouping(),
                        arg.time_domain().clone(),
                        arg.location().clone(),
                        None,
                    ));
                    Ok(arg.with_value(ast_dfg))
                })
                .try_collect()?;

            let args: Vec<_> = if function.is_aggregation() {
                // If the function is an aggregation, we may need to flatten the window.
                dfg.enter_env();
                dfg.bind("$condition_input", args[0].inner().clone());

                let window = &expr.unwrap().args()[1];
                let (condition, duration) = match window.op() {
                    ExprOp::Call(window_name) => {
                        flatten_window_args(window_name, window, dfg, data_context, diagnostics)?
                    }
                    ExprOp::Literal(v) if v.inner() == &LiteralValue::Null => {
                        // Unwindowed aggregations just use nulls
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

                        (null_arg.clone(), null_arg)
                    }
                    unexpected => anyhow::bail!("expected window, found {:?}", unexpected),
                };

                dfg.exit_env();
                // [agg_input, condition, duration]
                vec![args[0].clone(), condition, duration]
            } else if function.name() == "when" || function.name() == "if" {
                dfg.enter_env();
                dfg.bind("$condition_input", args[1].inner().clone());

                let condition = expr.unwrap().args()[0]
                    .as_ref()
                    .try_map(|condition| ast_to_dfg(data_context, dfg, diagnostics, condition))?;

                dfg.exit_env();
                // [condition, value]
                vec![condition, args[1].clone()]
            } else {
                args
            };

            function.create_dfg_node(
                function_name.location(),
                data_context,
                dfg,
                diagnostics,
                &args,
                instantiated_result_type,
                grouping,
            )
        }
        ExprOp::Pipe(_) => Err(anyhow!("Unreachable: Pipe expression handled above")),
        ExprOp::Let(_, _) => Err(anyhow!("Unreachable: Let expression handled above")),
        ExprOp::Error => Err(anyhow!("Unreachable: Error expression handled above")),
        ExprOp::Record(fields, location) => {
            record_to_dfg(data_context, location, dfg, diagnostics, fields, arguments)
        }
        ExprOp::ExtendRecord(location) => extend_record_to_dfg(
            data_context,
            location,
            dfg,
            diagnostics,
            arguments,
            argument_types,
        ),
        ExprOp::RemoveFields(location) => select_remove_fields(
            SelectOrRemove::Remove,
            location,
            dfg,
            diagnostics,
            arguments,
            argument_types,
        ),
        ExprOp::SelectFields(location) => select_remove_fields(
            SelectOrRemove::Select,
            location,
            dfg,
            diagnostics,
            arguments,
            argument_types,
        ),
    }
}

// Verify that the arguments are compatibly partitioned.
fn verify_same_partitioning(
    data_context: &DataContext,
    diagnostics: &mut DiagnosticCollector<'_>,
    operation: &Located<&'_ str>,
    arguments: &Resolved<Located<AstDfgRef>>,
) -> anyhow::Result<Option<GroupId>> {
    let groupings = arguments
        .iter()
        .filter_map(|arg| arg.grouping().map(|grouping| arg.with_value(grouping)))
        .unique();
    match groupings.at_most_one() {
        Ok(None) => Ok(None),
        Ok(Some(grouping)) => Ok(Some(grouping.into_inner())),
        Err(unique_groupings) => {
            let mut builder = DiagnosticCode::IncompatibleGrouping.builder().with_label(
                operation.location().primary_label().with_message(format!(
                    "Incompatible grouping for operation '{}'",
                    operation.inner()
                )),
            );

            for grouping in unique_groupings {
                let grouping_name = data_context
                    .group_info(*grouping.inner())
                    .context("Missing group id")?
                    .name();

                // TODO: Include the problematic tables?
                builder = builder.with_label(
                    grouping
                        .location()
                        .secondary_label()
                        .with_message(format!("Grouping: '{grouping_name}'")),
                )
            }
            builder.emit(diagnostics);

            // Treat incompatible grouping as ungrouped. This should prevent
            // cascading errors.
            Ok(None)
        }
    }
}

fn cast_if_needed(
    dfg: &mut Dfg,
    value: Id,
    value_type: &FenlType,
    expected_type: &FenlType,
) -> anyhow::Result<Id> {
    match (value_type, expected_type) {
        // Cast from error to anything produces an error. Value is already an error.
        (FenlType::Error, _) => Ok(value),
        // Cast from anything to the error type produces the previous value.
        // TODO: This maybe should create an error node and return it.
        (_, FenlType::Error) => Ok(value),
        (actual, expected) if actual == expected => Ok(value),
        // Ensures that map types with the same inner types are compatible, regardless of the (arbitary) field naming.
        (FenlType::Concrete(DataType::Map(s, _)), FenlType::Concrete(DataType::Map(s2, _)))
            if map_types_are_equal(s, s2) =>
        {
            Ok(value)
        }
        (FenlType::Concrete(DataType::Null), FenlType::Window) => Ok(value),
        (
            FenlType::Concrete(DataType::Struct(actual_fields)),
            FenlType::Concrete(DataType::Struct(expected_fields)),
        ) => {
            // TODO: We could (maybe) relax this to fill in null values if the expected
            // record is "wider" (has more fields).
            anyhow::ensure!(
                actual_fields.len() == expected_fields.len(),
                "Unable to cast between records with different number of fields",
            );

            let mut args = SmallVec::with_capacity(actual_fields.len() * 2);
            for (actual, expected) in izip!(actual_fields, expected_fields) {
                let expected_type = FenlType::Concrete(expected.data_type().clone());
                let field_name = dfg.add_string_literal(expected.name())?;
                let field = dfg.add_expression(
                    Expression::Inst(InstKind::FieldRef),
                    smallvec![value, field_name],
                )?;
                let actual_type = FenlType::Concrete(actual.data_type().clone());
                let value = cast_if_needed(dfg, field, &actual_type, &expected_type)?;

                args.push(field_name);
                args.push(value);
            }

            dfg.add_expression(Expression::Inst(InstKind::Record), args)
        }
        (_, FenlType::Concrete(dt)) => Ok(dfg.add_expression(
            Expression::Inst(InstKind::Cast(dt.clone())),
            smallvec![value],
        )?),
        (_, _) => {
            // Note: Only supports casting on `DataTypes`
            Err(anyhow!(
                "Attempting to add unsupported cast from {} to {}",
                value_type,
                expected_type
            ))
        }
    }
}

// When constructing the concrete map during inference, we use arbitary names for the inner data
// fields since we don't have access to the user's naming patterns there.
// By comparing the map types based on just the inner types, we can ensure that the types are
// still treated as equal.
fn map_types_are_equal(a: &FieldRef, b: &FieldRef) -> bool {
    match (a.data_type(), b.data_type()) {
        (DataType::Struct(a_fields), DataType::Struct(b_fields)) => {
            assert_eq!(a_fields.len() == 2, a_fields.len() == b_fields.len());
            a_fields[0].data_type() == b_fields[0].data_type()
                && a_fields[1].data_type() == b_fields[1].data_type()
        }
        _ => panic!("expected struct in map"),
    }
}

pub(crate) fn is_any_new(dfg: &mut Dfg, arguments: &[Located<AstDfgRef>]) -> anyhow::Result<Id> {
    let mut argument_is_new = arguments.iter().map(|a| a.is_new()).unique();
    let mut result = argument_is_new
        .next()
        .context("at least one argument expected")?;
    for next in argument_is_new {
        result = dfg.add_instruction(InstOp::LogicalOr, smallvec![result, next])?;
    }
    Ok(result)
}

fn add_literal(
    dfg: &mut Dfg,
    value: Id,
    value_type: FenlType,
    location: Location,
) -> anyhow::Result<AstDfgRef> {
    let is_new = dfg.add_literal(false)?;
    Ok(Rc::new(AstDfg::new(
        value,
        is_new,
        value_type,
        None,
        TimeDomain::literal(),
        location,
        None,
    )))
}

fn missing_field_diagnostic(
    fields: &[FieldRef],
    field_name: &str,
    location: &Location,
) -> DiagnosticBuilder {
    DiagnosticCode::IllegalFieldRef
        .builder()
        .with_label(
            // If the base is a struct without the field, the field reference is the
            // primary problem.
            location
                .primary_label()
                .with_message(format!("No field named '{field_name}'")),
        )
        .with_note(if fields.is_empty() {
            "No fields available on base record".to_owned()
        } else {
            let candidates = crate::nearest_matches::nearest_matches(
                field_name,
                fields.iter().map(|f| f.name()),
            );
            format!(
                "Nearest fields: {}",
                candidates
                    .iter()
                    .format_with(", ", |name, f| f(&format_args!("'{name}'")))
            )
        })
}
