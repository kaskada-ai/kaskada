use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::{DataType, Field, FieldRef};
use hashbrown::HashSet;
use itertools::{izip, Itertools};
use smallvec::{smallvec, SmallVec};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::InstKind;
use sparrow_syntax::{ArgVec, FenlType, Located, Location, Resolved};

use crate::ast_to_dfg::{is_any_new, missing_field_diagnostic, verify_same_partitioning};
use crate::dfg::{Dfg, Expression};
use crate::time_domain::{combine_time_domains, TimeDomain};
use crate::{
    AstDfg, AstDfgRef, DataContext, DiagnosticBuilder, DiagnosticCode, DiagnosticCollector,
};

pub(super) fn record_to_dfg(
    data_context: &DataContext,
    location: &Location,
    dfg: &mut Dfg,
    diagnostics: &mut DiagnosticCollector<'_>,
    fields: &ArgVec<Located<String>>,
    arguments: Resolved<Located<AstDfgRef>>,
) -> anyhow::Result<AstDfgRef> {
    // Check that record is non-empty
    let is_empty = {
        if arguments.is_empty() {
            DiagnosticCode::InvalidArguments
                .builder()
                .with_label(
                    location
                        .primary_label()
                        .with_message("Record must be non-empty"),
                )
                .emit(diagnostics);
        }
        arguments.is_empty()
    };
    // Check for duplicate fields.
    let has_duplicates = {
        let mut has_duplicates = false;
        fields
            .iter()
            .into_group_map_by(|l| l.inner())
            .iter()
            .filter(|(_, locations)| locations.len() > 1)
            .sorted_by_key(|(_, locations)| locations.iter().min().unwrap())
            .for_each(|(name, locations)| {
                has_duplicates = true;
                DiagnosticCode::DuplicateFieldNames
                    .builder()
                    .with_label(
                        locations[0]
                            .location()
                            .primary_label()
                            .with_message(format!("Field '{name}' defined multiple times")),
                    )
                    .with_labels(
                        locations[1..]
                            .iter()
                            .map(|l| l.location().secondary_label())
                            .collect(),
                    )
                    .emit(diagnostics)
            });
        has_duplicates
    };

    let has_non_concrete = {
        let mut has_non_concrete = false;
        for (index, value) in arguments.iter().enumerate() {
            match value.value_type() {
                FenlType::Concrete(_) => {}
                FenlType::Error => {
                    // Non-concrete, but the error is already reported.
                    has_non_concrete = true;
                }
                non_concrete => {
                    DiagnosticCode::InvalidArgumentType
                        .builder()
                        .with_label(value.location().primary_label().with_message(format!(
                            "Field '{}' has invalid type {}",
                            arguments.parameter_name(index),
                            non_concrete
                        )))
                        .emit(diagnostics);
                    has_non_concrete = true;
                }
            }
        }
        has_non_concrete
    };

    if is_empty || has_duplicates || has_non_concrete {
        return Ok(dfg.error_node());
    }

    let operation = Located::new("record creation", location.clone());

    let grouping = verify_same_partitioning(data_context, diagnostics, &operation, &arguments)?;

    let is_new = is_any_new(dfg, arguments.values())?;

    let time_domain = combine_time_domains(location, arguments.values(), data_context)?
        .unwrap_or_else(|diagnostic| {
            diagnostic.emit(diagnostics);
            TimeDomain::error()
        });

    // For each argument (field) in the record, we need two arguments to the
    // instruction the name and the value.
    let mut instruction_args = SmallVec::with_capacity(arguments.len() * 2);
    let mut struct_fields = Vec::with_capacity(arguments.len());
    for (field, value) in izip!(fields.iter(), arguments.iter()) {
        let name = field.inner();

        let field_type = value.value_type();
        // Errors shouldn't occur here because we checked arguments for
        // error type earlier.
        let field_type = field_type
            .arrow_type()
            .with_context(|| format!("Expected field to be concrete type, but was {field_type:?}"))?
            .clone();

        struct_fields.push(Field::new(name, field_type, true));

        let name = dfg.add_string_literal(name)?;
        instruction_args.push(name);
        instruction_args.push(value.value());
    }
    instruction_args.shrink_to_fit();

    let value_type = DataType::Struct(struct_fields.into()).into();

    // Create the value after the fields since this takes ownership of the names.
    let value = dfg.add_expression(Expression::Inst(InstKind::Record), instruction_args)?;

    Ok(Arc::new(AstDfg::new(
        value,
        is_new,
        value_type,
        grouping,
        time_domain,
        location.clone(),
        Some(
            arguments
                .values()
                .iter()
                .map(|i| i.inner().clone())
                .collect(),
        ),
    )))
}

pub(super) fn extend_record_to_dfg(
    data_context: &DataContext,
    location: &Location,
    dfg: &mut Dfg,
    diagnostics: &mut DiagnosticCollector<'_>,
    arguments: Resolved<Located<AstDfgRef>>,
    argument_types: Resolved<Located<FenlType>>,
) -> anyhow::Result<AstDfgRef> {
    let operation = Located::new("record extension", location.clone());
    let grouping = verify_same_partitioning(data_context, diagnostics, &operation, &arguments)?;

    let extension_node = &arguments[0];
    let base_node = &arguments[1];

    let extension_type = &argument_types[0];
    let base_type = &argument_types[1];

    if base_type.is_error() || extension_type.is_error() {
        return Ok(dfg.error_node());
    }

    let extension_fields =
        if let FenlType::Concrete(DataType::Struct(fields)) = extension_type.inner() {
            fields.iter().map(|f| (f, extension_node))
        } else {
            DiagnosticCode::InvalidArgumentType
                .builder()
                .with_label(
                    arguments[0]
                        .location()
                        .primary_label()
                        .with_message(format!(
                            "{} argument to extend must be record, but was {}",
                            arguments.names()[0],
                            argument_types[0]
                        )),
                )
                .emit(diagnostics);
            return Ok(dfg.error_node());
        };

    let base_fields = if let FenlType::Concrete(DataType::Struct(fields)) = base_type.inner() {
        fields.iter().map(|f| (f, base_node))
    } else {
        DiagnosticCode::InvalidArgumentType
            .builder()
            .with_label(
                arguments[1]
                    .location()
                    .primary_label()
                    .with_message(format!(
                        "{} argument to extend must be record, but was {}",
                        arguments.names()[1],
                        argument_types[1]
                    )),
            )
            .emit(diagnostics);
        return Ok(dfg.error_node());
    };

    // If fields are duplicated, the field from the extension takes precedent.
    let unique_fields = extension_fields
        .chain(base_fields)
        .unique_by(|(f, _)| f.name());

    let mut combined_fields = Vec::new();
    let mut record_args = SmallVec::new();
    let mut ast_fields = Vec::new();

    for (field, node) in unique_fields {
        combined_fields.push(field.clone());

        let field_name = dfg.add_string_literal(field.name())?;
        let field_ref = dfg.add_expression(
            Expression::Inst(InstKind::FieldRef),
            smallvec![node.value(), field_name],
        )?;
        record_args.push(field_name);
        record_args.push(field_ref);
        ast_fields.push(node.inner().clone());
    }

    combined_fields.shrink_to_fit();
    record_args.shrink_to_fit();

    // Build a record construction expression from the RHS & LHS fields.
    let is_new = is_any_new(dfg, arguments.values())?;
    let value = dfg.add_expression(Expression::Inst(InstKind::Record), record_args)?;
    let value_type = DataType::Struct(combined_fields.into()).into();

    let time_domain = combine_time_domains(location, arguments.values(), data_context)?
        .unwrap_or_else(|diagnostic| {
            diagnostic.emit(diagnostics);
            TimeDomain::error()
        });

    Ok(Arc::new(AstDfg::new(
        value,
        is_new,
        value_type,
        grouping,
        time_domain,
        location.clone(),
        Some(ast_fields),
    )))
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
pub(super) enum SelectOrRemove {
    Select,
    Remove,
}

impl SelectOrRemove {
    fn result_len(&self, record_fields: usize, field_names: usize) -> usize {
        match self {
            SelectOrRemove::Select => field_names,
            SelectOrRemove::Remove => record_fields.saturating_sub(field_names),
        }
    }

    fn include(&self, field_names: &HashSet<String>, field_name: &str) -> bool {
        (self == &SelectOrRemove::Select) == field_names.contains(field_name)
    }
}

impl std::fmt::Display for SelectOrRemove {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectOrRemove::Select => f.write_str("select_fields"),
            SelectOrRemove::Remove => f.write_str("remove_fields"),
        }
    }
}

pub(super) fn select_remove_fields(
    select: SelectOrRemove,
    location: &Location,
    dfg: &mut Dfg,
    diagnostics: &mut DiagnosticCollector<'_>,
    arguments: Resolved<Located<AstDfgRef>>,
    argument_types: Resolved<Located<FenlType>>,
) -> anyhow::Result<AstDfgRef> {
    let record = &arguments[0];
    let record_type = &argument_types[0];

    // 1. Determine the actual fields of the record.
    if record_type.is_error() {
        return Ok(dfg.error_node());
    }

    let input_fields = if let Some(input_fields) = record_type.record_fields() {
        input_fields
    } else {
        DiagnosticCode::InvalidArgumentType
            .builder()
            .with_label(record.location().primary_label().with_message(format!(
                "Expected 'record' argument to '{select}' to be a struct, but was {record_type}"
            )))
            .emit(diagnostics);
        return Ok(dfg.error_node());
    };

    // 2. Validate and extract the field names (remaining arguments).
    let field_name_arguments = arguments
        .values()
        .iter()
        .zip(argument_types.values())
        .skip(1);
    let field_name_arguments =
        match extract_field_name_arguments(dfg, select, input_fields, field_name_arguments) {
            Ok(field_names) => field_names,
            Err(field_diagnostics) => {
                diagnostics.collect_all(field_diagnostics);
                return Ok(dfg.error_node());
            }
        };

    // 3. Construct the set of fields and arguments to the record expression.
    let mut result_fields = Vec::new();

    // For each argument (field) in the record, we need two arguments to the
    // instruction the name and the value.
    let result_field_len = select.result_len(input_fields.len(), field_name_arguments.len());
    let mut record_args = SmallVec::with_capacity(result_field_len * 2);
    let mut ast_fields = Vec::new();

    for input_field in input_fields {
        let included = select.include(&field_name_arguments, input_field.name());
        if included {
            let field_name = dfg.add_string_literal(input_field.name())?;

            result_fields.push(input_field.clone());

            record_args.push(field_name);
            let field_node = dfg.add_expression(
                Expression::Inst(InstKind::FieldRef),
                smallvec![record.value(), field_name],
            )?;
            record_args.push(field_node);
            ast_fields.push(record.inner().clone());
        }
    }

    // If all fields have been excluded from the result, return an error node
    if record_args.is_empty() {
        DiagnosticCode::InvalidArguments
            .builder()
            .with_label(
                location
                    .primary_label()
                    .with_message("Removed all fields from input record."),
            )
            .emit(diagnostics);
        return Ok(dfg.error_node());
    }

    record_args.shrink_to_fit();

    // 4. Return the record instruction.
    let value = dfg.add_expression(Expression::Inst(InstKind::Record), record_args)?;
    let value_type = FenlType::Concrete(DataType::Struct(result_fields.into()));

    Ok(Arc::new(AstDfg::new(
        value,
        record.is_new(),
        value_type,
        record.grouping(),
        record.time_domain().clone(),
        record.location().clone(),
        Some(ast_fields),
    )))
}

fn extract_field_name_arguments<'a>(
    dfg: &Dfg,
    select: SelectOrRemove,
    input_fields: &[FieldRef],
    arguments: impl Iterator<Item = (&'a Located<AstDfgRef>, &'a Located<FenlType>)>,
) -> Result<HashSet<String>, Vec<DiagnosticBuilder>> {
    let mut errors = Vec::new();
    let mut field_names = HashSet::new();
    let valid_field_names: HashSet<_> = input_fields.iter().map(|f| f.name()).collect();
    for (argument, argument_type) in arguments {
        match dfg.literal(argument.value()) {
            None => errors.push(
                DiagnosticCode::InvalidNonConstArgument
                    .builder()
                    .with_label(argument.location().primary_label().with_message(format!(
                    "Argument 'field' to '{select}' must be constant non-null string, but was not \
                         constant",
                ))),
            ),
            Some(ScalarValue::Null) | Some(ScalarValue::Utf8(None)) => {
                errors.push(DiagnosticCode::InvalidArguments.builder().with_label(
                    argument.location().primary_label().with_message(format!(
                    "Argument 'field' to '{select}' must be constant non-null string, but was null"
                )),
                ))
            }
            Some(ScalarValue::Utf8(Some(s))) if !valid_field_names.contains(s) => errors.push(
                missing_field_diagnostic(input_fields, s, argument.location()),
            ),
            Some(ScalarValue::Utf8(Some(s))) => {
                field_names.insert(s.to_owned());
            }
            Some(_) => {
                errors.push(DiagnosticCode::InvalidArgumentType.builder().with_label(
                    argument.location().primary_label().with_message(format!(
                        "Argument 'field' to '{select}' must be string, but was {}",
                        argument_type.inner()
                    )),
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(field_names)
    } else {
        Err(errors)
    }
}
