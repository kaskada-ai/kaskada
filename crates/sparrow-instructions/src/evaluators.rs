use crate::{InstKind, InstOp, ValueRef};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use itertools::Itertools;

use crate::evaluators::macros::{
    create_float_evaluator, create_number_evaluator, create_ordered_evaluator,
    create_signed_evaluator, create_typed_evaluator,
};
use crate::{ColumnarValue, ComputeStore, GroupingIndices};

pub mod aggregation;
mod cast;
mod comparison;
mod equality;
mod field_ref;
mod general;
mod json_field;
mod list;
mod logical;
mod macros;
mod map;
mod math;
mod record;
mod string;
mod time;

pub use aggregation::*;
pub use cast::*;
use comparison::*;
use equality::*;
use field_ref::*;
use general::*;
use json_field::*;
use list::*;
use logical::*;
use map::*;
use math::*;
use record::*;
use string::*;
use time::*;

/// Represents static information for an evaluator.
#[derive(Debug)]
pub struct StaticInfo<'a> {
    inst_kind: &'a InstKind,
    pub args: Vec<StaticArg>,
    pub result_type: &'a DataType,
}

impl<'a> StaticInfo<'a> {
    pub fn new(inst_kind: &'a InstKind, args: Vec<StaticArg>, result_type: &'a DataType) -> Self {
        Self {
            inst_kind,
            args,
            result_type,
        }
    }
}

impl<'a> StaticInfo<'a> {
    fn unpack_argument(mut self) -> anyhow::Result<ValueRef> {
        let num_args = self.args.len();
        if self.args.len() == 1 {
            Ok(self.args.swap_remove(0).value_ref)
        } else {
            anyhow::bail!(
                "Unable to unpack arguments for '{}': expected {} but got {}",
                self.inst_kind,
                1,
                num_args
            )
        }
    }

    fn unpack_arguments<T: itertools::traits::HomogeneousTuple<Item = ValueRef>>(
        self,
    ) -> anyhow::Result<T> {
        let num_args = self.args.len();
        let mut args = self.args.into_iter().map(|arg| arg.value_ref);
        match args.next_tuple() {
            Some(t) => Ok(t),
            None => anyhow::bail!(
                "Unable to unpack arguments for '{}': expected {} but got {}",
                self.inst_kind,
                T::num_items(),
                num_args
            ),
        }
    }
}

/// Represents the static information about an argument.
///
/// This includes the type and the associated value ref, as well
/// as whether the argument is a literal.
#[derive(Debug, Clone)]
pub struct StaticArg {
    pub value_ref: ValueRef,
    pub data_type: DataType,
}

impl StaticArg {
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub(super) fn is_literal(&self) -> bool {
        self.value_ref.literal_value().is_some()
    }

    pub(super) fn is_literal_null(&self) -> bool {
        self.value_ref.is_literal_null()
    }
}

pub trait RuntimeInfo {
    fn value(&self, arg: &ValueRef) -> anyhow::Result<ColumnarValue>;
    fn grouping(&self) -> &GroupingIndices;
    fn time_column(&self) -> ColumnarValue;
    fn storage(&self) -> Option<&ComputeStore>;
    fn num_rows(&self) -> usize;
}

/// Trait for evaluating instructions.
pub trait Evaluator: Send + Sync {
    /// Evaluate the function with the given runtime info.
    ///
    /// While we may like to specialize this as `impl RuntimeInfo` that is not
    /// compatible with the use of this as `Box<dyn Evaluator>`. So, we allow
    /// dynamic dispatch to any `RuntimeInfo` reference.
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef>;

    /// Return the state token managing the evaluator state, if any.
    fn state_token(&self) -> Option<&dyn crate::StateToken> {
        None
    }

    /// Return the state token managing the evaluator state, if any.
    fn state_token_mut(&mut self) -> Option<&mut dyn crate::StateToken> {
        None
    }
}

/// Trait for creating evaluators.
///
/// Separated to allow factories to create specialized versions of their
/// evaluators based on some static info. For example, an aggregation with
/// window arguments will create a windowed version of the evaluator.
pub trait EvaluatorFactory {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>>;
}

pub fn create_evaluator(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
    match info.inst_kind {
        InstKind::Simple(op) => create_simple_evaluator(*op, info),
        InstKind::FieldRef => Ok(FieldRefEvaluator::try_new(info)?),
        InstKind::Cast(cast_type) => {
            assert_eq!(info.result_type, cast_type);
            Ok(CastEvaluator::try_new(info)?)
        }
        InstKind::Record => Ok(RecordEvaluator::try_new(info)?),
        InstKind::Udf(udf) => Ok(udf.make_evaluator(info)),
    }
}

/// Placeholder struct for unsupported evaluators.
struct UnsupportedEvaluator;

impl Evaluator for UnsupportedEvaluator {
    fn evaluate(&mut self, _info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        anyhow::bail!("Unsupported evaluator")
    }
}

impl EvaluatorFactory for UnsupportedEvaluator {
    fn try_new(_info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        anyhow::bail!("Unsupported evaluator")
    }
}

/// Create the evaluator for a given instruction and arguments.
fn create_simple_evaluator(
    op: InstOp,
    mut info: StaticInfo<'_>,
) -> anyhow::Result<Box<dyn Evaluator>> {
    match op {
        InstOp::Add => {
            create_number_evaluator!(&info.args[0].data_type, AddEvaluator, info)
        }
        InstOp::AddTime => AddTimeEvaluator::try_new(info),
        InstOp::Ceil => CeilEvaluator::try_new(info),
        InstOp::Clamp => {
            create_number_evaluator!(&info.args[0].data_type, ClampEvaluator, info)
        }
        InstOp::Coalesce => CoalesceEvaluator::try_new(info),
        InstOp::Collect => {
            create_typed_evaluator!(
                &info.args[0].data_type,
                CollectPrimitiveEvaluator,
                CollectStructEvaluator,
                UnsupportedEvaluator,
                UnsupportedEvaluator,
                CollectBooleanEvaluator,
                CollectStringEvaluator,
                info
            )
        }
        InstOp::CountIf => CountIfEvaluator::try_new(info),
        InstOp::DayOfMonth => DayOfMonthEvaluator::try_new(info),
        InstOp::DayOfMonth0 => DayOfMonth0Evaluator::try_new(info),
        InstOp::DayOfYear => DayOfYearEvaluator::try_new(info),
        InstOp::DayOfYear0 => DayOfYear0Evaluator::try_new(info),
        InstOp::Days => DaysEvaluator::try_new(info),
        InstOp::DaysBetween => DaysBetweenEvaluator::try_new(info),
        InstOp::Div => {
            create_number_evaluator!(&info.args[0].data_type, DivEvaluator, info)
        }
        InstOp::Eq => EqEvaluatorFactory::try_new(info),
        InstOp::Exp => {
            create_float_evaluator!(&info.args[0].data_type, ExpEvaluator, info)
        }
        InstOp::First => {
            create_typed_evaluator!(
                &info.args[0].data_type,
                ArrowAggEvaluator,
                UnsupportedEvaluator,
                FirstListEvaluator,
                FirstMapEvaluator,
                FirstBooleanEvaluator,
                FirstStringEvaluator,
                FirstPrimitive,
                info
            )
        }
        InstOp::Flatten => FlattenEvaluator::try_new(info),
        InstOp::Floor => FloorEvaluator::try_new(info),
        InstOp::Get => GetEvaluator::try_new(info),
        InstOp::Index => IndexEvaluator::try_new(info),
        InstOp::Gt => match (info.args[0].is_literal(), info.args[1].is_literal()) {
            (_, true) => {
                create_ordered_evaluator!(&info.args[0].data_type, GtScalarEvaluator, info)
            }
            (true, false) => {
                // Flip arguments: `a > b == b < a`, and gets the scalar on the right.
                info.args.reverse();
                create_ordered_evaluator!(&info.args[0].data_type, LtScalarEvaluator, info)
            }
            (false, false) => {
                create_ordered_evaluator!(&info.args[0].data_type, GtEvaluator, info)
            }
        },
        InstOp::Gte => match (info.args[0].is_literal(), info.args[1].is_literal()) {
            (_, true) => {
                create_ordered_evaluator!(&info.args[0].data_type, GteScalarEvaluator, info)
            }
            (true, false) => {
                // Flip arguments: `a >= b == b <= a`, and gets the scalar on the right.
                info.args.reverse();
                create_ordered_evaluator!(&info.args[0].data_type, LteScalarEvaluator, info)
            }
            (false, false) => {
                create_ordered_evaluator!(&info.args[0].data_type, GteEvaluator, info)
            }
        },
        InstOp::Hash => HashEvaluator::try_new(info),
        InstOp::If => IfEvaluator::try_new(info),
        InstOp::IsValid => IsValidEvaluator::try_new(info),
        // HACK: the `json` function is converted into the `json_field` instruction during
        // simplification of the dfg. This is not a pattern intended to be followed; it's
        // weird how both the `InstOp::Json` and `InstOp::JsonField` exist in the dfg and
        // rely on simplification for conversion.
        InstOp::Json => anyhow::bail!("No evaluator defined for json function"),
        InstOp::JsonField => JsonFieldEvaluator::try_new(info),
        InstOp::Last => {
            create_typed_evaluator!(
                &info.args[0].data_type,
                ArrowAggEvaluator,
                UnsupportedEvaluator,
                LastListEvaluator,
                LastMapEvaluator,
                LastBooleanEvaluator,
                LastStringEvaluator,
                LastPrimitive,
                info
            )
        }
        InstOp::Len => LenEvaluator::try_new(info),
        InstOp::ListLen => ListLenEvaluator::try_new(info),
        InstOp::LogicalAnd => LogicalAndKleeneEvaluator::try_new(info),
        InstOp::LogicalOr => LogicalOrKleeneEvaluator::try_new(info),
        InstOp::Lower => LowerEvaluator::try_new(info),
        InstOp::Lt => match (info.args[0].is_literal(), info.args[1].is_literal()) {
            (_, true) => {
                create_ordered_evaluator!(&info.args[0].data_type, LtScalarEvaluator, info)
            }
            (true, false) => {
                // Flip arguments: `a < b == b > a`, and gets the scalar on the right.
                info.args.reverse();
                create_ordered_evaluator!(&info.args[0].data_type, GtScalarEvaluator, info)
            }
            (false, false) => {
                create_ordered_evaluator!(&info.args[0].data_type, LtEvaluator, info)
            }
        },
        InstOp::Lte => match (info.args[0].is_literal(), info.args[1].is_literal()) {
            (_, true) => {
                create_ordered_evaluator!(&info.args[0].data_type, LteScalarEvaluator, info)
            }
            (true, false) => {
                // Flip arguments: `a <= b == b >= a`, and gets the scalar on the right.
                info.args.reverse();
                create_ordered_evaluator!(&info.args[0].data_type, GteScalarEvaluator, info)
            }
            (false, false) => {
                create_ordered_evaluator!(&info.args[0].data_type, LteEvaluator, info)
            }
        },
        InstOp::Max => {
            create_ordered_evaluator!(&info.args[0].data_type, ArrowAggEvaluator, Max, info)
        }
        InstOp::Mean => {
            create_number_evaluator!(&info.args[0].data_type, ArrowAggEvaluator, Mean, info)
        }
        InstOp::Min => {
            create_ordered_evaluator!(&info.args[0].data_type, ArrowAggEvaluator, Min, info)
        }
        InstOp::MonthOfYear => MonthOfYearEvaluator::try_new(info),
        InstOp::MonthOfYear0 => MonthOfYear0Evaluator::try_new(info),
        InstOp::Months => MonthsEvaluator::try_new(info),
        InstOp::MonthsBetween => MonthsBetweenEvaluator::try_new(info),
        InstOp::Mul => {
            create_number_evaluator!(&info.args[0].data_type, MulEvaluator, info)
        }
        InstOp::Neg => {
            create_signed_evaluator!(&info.args[0].data_type, NegEvaluator, info)
        }
        InstOp::Neq => NeqEvaluatorFactory::try_new(info),
        InstOp::Not => NotEvaluator::try_new(info),
        InstOp::NullIf => NullIfEvaluator::try_new(info),
        InstOp::Powf => {
            create_float_evaluator!(&info.args[0].data_type, PowfEvaluator, info)
        }
        InstOp::Round => RoundEvaluator::try_new(info),
        InstOp::Seconds => SecondsEvaluator::try_new(info),
        InstOp::SecondsBetween => SecondsBetweenEvaluator::try_new(info),
        InstOp::Sub => {
            create_number_evaluator!(&info.args[0].data_type, SubEvaluator, info)
        }
        InstOp::Substring => SubstringEvaluator::try_new(info),
        InstOp::Sum => {
            create_number_evaluator!(&info.args[0].data_type, ArrowAggEvaluator, Sum, info)
        }
        InstOp::TimeOf => TimeOfEvaluator::try_new(info),
        InstOp::Upper => UpperEvaluator::try_new(info),
        InstOp::Union => UnionEvaluator::try_new(info),
        InstOp::Variance => {
            create_number_evaluator!(
                &info.args[0].data_type,
                ArrowAggEvaluator,
                SampleVariance,
                info
            )
        }
        InstOp::Year => YearEvaluator::try_new(info),
        InstOp::ZipMax => {
            create_number_evaluator!(&info.args[0].data_type, ZipMaxEvaluator, info)
        }
        InstOp::ZipMin => {
            create_number_evaluator!(&info.args[0].data_type, ZipMinEvaluator, info)
        }
    }
}
