use std::str::FromStr;

use anyhow::{anyhow, Context};
use arrow::datatypes::{DataType, FieldRef};
use egg::{Id, Subst, Var};
use smallvec::smallvec;
use sparrow_instructions::InstKind;

use crate::dfg::{ChildrenVec, Dfg, DfgPattern, Expression};

/// Implements pushdown of instructions to the fields of records.
///
/// Used to implement operations such as `last` and `first` on records
/// in terms of `last` and `first` on the individual fields.
#[derive(Debug)]
pub(crate) struct Pushdown {
    /// The argument that the pushdown should be performed on.
    ///
    /// This is used to find the type that the pushdown is performed on.
    pushdown_on: usize,
    /// The pattern to use on primitive types.
    primitive_pattern: DfgPattern,
    /// The pattern to use on each field of a record.
    record_field_pattern: DfgPattern,
    /// The pattern to use after assembling a record containing the pushed-down
    /// fields.
    record_result_pattern: DfgPattern,
}

impl Pushdown {
    /// Configure how an operation is pushed down to primitives.
    ///
    /// Configured with the parameter whose type should "drive" the pushdown and
    /// the patterns to use for pushing the operation to a record (fields and
    /// final result).
    ///
    /// The primitive pattern may reference `?input` to get the value of the
    /// input.
    ///
    /// The `record_field_pattern` may reference `input_record` and
    /// `input_field` to get the original input record and the field being
    /// worked on, respectively.
    ///
    /// The `record_result_pattern` may reference `input_record` and
    /// `result_record` to get the original input record and the newly
    /// created pushed-down version.
    ///
    /// All arguments other than the argument used for `pushdown_on` may also be
    /// referenced by name, as `?name`.
    pub(super) fn try_new(
        pushdown_on: usize,
        primitive_pattern: &str,
        record_field_pattern: &str,
        record_result_pattern: &str,
    ) -> anyhow::Result<Self> {
        let primitive_pattern =
            DfgPattern::from_str(primitive_pattern).context("primitive_pattern")?;
        let record_field_pattern =
            DfgPattern::from_str(record_field_pattern).context("record_field_pattern")?;
        let recurse_on_input_field =
            Var::from_str("?recurse_on_input_field").context("valid var")?;

        // Make sure we recursively expand things. If this isn't true, then the
        // pattern only applies the rewrites one level deep.
        anyhow::ensure!(
            record_field_pattern.references(&recurse_on_input_field),
            "Record field pattern must include ?recurse_on_input_field"
        );

        let record_result_pattern =
            DfgPattern::from_str(record_result_pattern).context("record_result_pattern")?;

        Ok(Self {
            pushdown_on,
            primitive_pattern,
            record_field_pattern,
            record_result_pattern,
        })
    }

    pub(super) fn pushdown_on(&self) -> usize {
        self.pushdown_on
    }

    pub(super) fn pushdown(
        &self,
        dfg: &mut Dfg,
        subst: &Subst,
        value: Id,
        value_type: &DataType,
    ) -> anyhow::Result<Id> {
        match value_type {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::List(..)
            | DataType::Map(..) => {
                let mut subst = subst.clone();
                subst.insert(
                    Var::from_str("?input_value").context("Failed to parse ?input_value")?,
                    value,
                );
                dfg.add_pattern(&self.primitive_pattern, &subst)
            }
            DataType::Struct(fields) => {
                let fields = fields.clone();
                self.pushdown_struct(dfg, subst, value, fields.as_ref())
            }
            unsupported => Err(anyhow!("Pushdown operation on type {:?}", unsupported)),
        }
    }

    fn pushdown_struct(
        &self,
        dfg: &mut Dfg,
        subst: &Subst,
        record: Id,
        fields: &[FieldRef],
    ) -> anyhow::Result<Id> {
        let mut args = ChildrenVec::with_capacity(fields.len() * 2);

        let mut field_subst = subst.clone();

        let field_var = Var::from_str("?input_field").context("Failed to parse ?input_field")?;
        let recursive_var = Var::from_str("?recurse_on_input_field")
            .context("Failed to parse ?recurse_on_input_field")?;
        field_subst.insert(
            Var::from_str("?input_record").context("Failed to parse ?input_record")?,
            record,
        );

        for field in fields {
            // T.a
            let field_name = dfg.add_string_literal(field.name())?;

            let field_ref = dfg.add_expression(
                Expression::Inst(InstKind::FieldRef),
                smallvec![record, field_name],
            )?;

            field_subst.insert(field_var, field_ref);

            let recurse = self.pushdown(dfg, subst, field_ref, field.data_type())?;
            field_subst.insert(recursive_var, recurse);

            args.push(field_name);
            args.push(dfg.add_pattern(&self.record_field_pattern, &field_subst)?);
        }

        // The result record.
        let result_record = dfg.add_expression(Expression::Inst(InstKind::Record), args)?;

        let mut result_subst = subst.clone();
        result_subst.insert(
            Var::from_str("?input_record").context("Failed to parse ?input_record")?,
            record,
        );
        result_subst.insert(
            Var::from_str("?result_record").context("Failed to parse ?result_record")?,
            result_record,
        );

        dfg.add_pattern(&self.record_result_pattern, &result_subst)
    }
}
