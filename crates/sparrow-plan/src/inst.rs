use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use enum_map::{enum_map, Enum, EnumMap};
use parse_display::{Display, FromStr};
use sparrow_syntax::{FeatureSetPart, FenlType, Signature};
use static_init::dynamic;
use strum::EnumProperty;

use crate::value::ValueRef;

/// The mode an instruction is being used in.
///
/// Affects which signature is used.
pub enum Mode {
    Dfg,
    Plan,
}

/// Enumeration of the instruction operations.
///
/// Instructions don't include any "payload" -- they are an operation applied
/// to 1 or more inputs that produces 1 output.
///
/// # TODO - Combine InstOp and InstKind
/// - `FieldRef` becomes an instruction with a `base` argument and a string
///   literal.
/// - `Record` becomes an instruction with N pairs of arguments -- the string
///   literal for the field name and the value.
/// - `Cast` becomes an instruction with one argument and uses the result type
///   of the instruction in the plan to indicate the destination type.
#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    Enum,
    strum_macros::EnumIter,
    strum_macros::EnumProperty,
    strum_macros::IntoStaticStr,
    Eq,
    FromStr,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[display(style = "snake_case")]
pub enum InstOp {
    #[strum(props(signature = "add(a: number, b: number) -> number"))]
    Add,
    #[strum(props(signature = "add_time(delta: timedelta, time: timestamp_ns) -> timestamp_ns"))]
    AddTime,
    #[strum(props(signature = "ceil(n: number) -> number"))]
    Ceil,
    #[strum(props(
        signature = "clamp(value: number, min: number = null, max: number = null) -> number"
    ))]
    Clamp,
    #[strum(props(signature = "coalesce(values+: any) -> any"))]
    Coalesce,
    #[strum(props(
        dfg_signature = "count_if(input: any, window: window = null) -> u32",
        plan_signature = "count_if(input: any, ticks: bool = null, slide_duration: i64 = null) -> \
                          u32"
    ))]
    CountIf,
    #[strum(props(signature = "day_of_month(time: timestamp_ns) -> u32"))]
    DayOfMonth,
    #[strum(props(signature = "day_of_month0(time: timestamp_ns) -> u32"))]
    DayOfMonth0,
    #[strum(props(signature = "day_of_year(time: timestamp_ns) -> u32"))]
    DayOfYear,
    #[strum(props(signature = "day_of_year0(time: timestamp_ns) -> u32"))]
    DayOfYear0,
    #[strum(props(signature = "days(days: i64) -> interval_days"))]
    Days,
    #[strum(props(
        signature = "days_between(t1: timestamp_ns, t2: timestamp_ns) -> interval_days"
    ))]
    DaysBetween,
    #[strum(props(signature = "div(a: number, b: number) -> number"))]
    Div,
    #[strum(props(signature = "eq(a: any, b: any) -> bool"))]
    Eq,
    #[strum(props(signature = "exp(power: f64) -> f64"))]
    Exp,
    #[strum(props(
        dfg_signature = "first(input: any, window: window = null) -> any",
        plan_signature = "first(input: any, ticks: bool = null, slide_duration: i64 = null) -> any"
    ))]
    First,
    #[strum(props(signature = "floor(n: number) -> number"))]
    Floor,
    #[strum(props(signature = "gt(a: ordered, b: ordered) -> bool"))]
    Gt,
    #[strum(props(signature = "gte(a: ordered, b: ordered) -> bool"))]
    Gte,
    #[strum(props(signature = "hash(input: any) -> u64"))]
    Hash,
    #[strum(props(signature = "if(condition: bool, value: any) -> any"))]
    If,
    #[strum(props(signature = "is_valid(input: any) -> bool"))]
    IsValid,
    // HACK: This instruction does not show up in the plan/does not have an evaluator.
    // It is converted to `JsonField` during simplification.
    #[strum(props(signature = "json(s: string) -> json"))]
    Json,
    #[strum(props(signature = "json_field(s: string, field: string) -> string"))]
    JsonField,
    #[strum(props(signature = "lag(n: i64, input: ordered) -> ordered"))]
    Lag,
    #[strum(props(
        dfg_signature = "last(input: any, window: window = null) -> any",
        plan_signature = "last(input: any, ticks: bool = null, slide_duration: i64 = null) -> any"
    ))]
    Last,
    #[strum(props(signature = "len(s: string) -> i32"))]
    Len,
    #[strum(props(signature = "logical_and(a: bool, b: bool) -> bool"))]
    LogicalAnd,
    #[strum(props(signature = "logical_or(a: bool, b: bool) -> bool"))]
    LogicalOr,
    #[strum(props(signature = "lower(s: string) -> string"))]
    Lower,
    #[strum(props(signature = "lt(a: ordered, b: ordered) -> bool"))]
    Lt,
    #[strum(props(signature = "lte(a: ordered, b: ordered) -> bool"))]
    Lte,
    #[strum(props(
        dfg_signature = "max(input: ordered, window: window = null) -> ordered",
        plan_signature = "max(input: ordered, ticks: bool = null, slide_duration: i64 = null) -> \
                          ordered"
    ))]
    Max,
    #[strum(props(
        dfg_signature = "max_by(measure: ordered, value: any, window: window = null) -> any",
        plan_signature = "max_by(measure: ordered, value: any, ticks: bool = null, slide_duration: i64 = null) -> any"
    ))]
    MaxBy,
    #[strum(props(
        dfg_signature = "mean(input: number, window: window = null) -> f64",
        plan_signature = "mean(input: number, ticks: bool = null, slide_duration: i64 = null) -> \
                          f64"
    ))]
    Mean,
    #[strum(props(
        dfg_signature = "min(input: ordered, window: window = null) -> ordered",
        plan_signature = "min(input: ordered, ticks: bool = null, slide_duration: i64 = null) -> \
                          ordered"
    ))]
    Min,
    #[strum(props(
        dfg_signature = "min_by(measure: ordered, value: any, window: window = null) -> any",
        plan_signature = "min_by(input: any, ticks: bool = null, slide_duration: i64 = null) -> any"
    ))]
    MinBy,
    #[strum(props(signature = "month_of_year(time: timestamp_ns) -> u32"))]
    MonthOfYear,
    #[strum(props(signature = "month_of_year0(time: timestamp_ns) -> u32"))]
    MonthOfYear0,
    #[strum(props(signature = "months(months: i64) -> interval_months"))]
    Months,
    #[strum(props(
        signature = "months_between(t1: timestamp_ns, t2: timestamp_ns) -> interval_months"
    ))]
    MonthsBetween,
    #[strum(props(signature = "mul(a: number, b: number) -> number"))]
    Mul,
    #[strum(props(signature = "neg(n: signed) -> signed"))]
    Neg,
    #[strum(props(signature = "neq(a: any, b: any) -> bool"))]
    Neq,
    #[strum(props(signature = "not(input: bool) -> bool"))]
    Not,
    #[strum(props(signature = "null_if(condition: bool, value: any) -> any"))]
    NullIf,
    #[strum(props(signature = "powf(base: f64, power: f64) -> f64"))]
    Powf,
    #[strum(props(signature = "round(n: number) -> number"))]
    Round,
    #[strum(props(signature = "seconds(seconds: i64) -> duration_s"))]
    Seconds,
    #[strum(props(
        signature = "seconds_between(t1: timestamp_ns, t2: timestamp_ns) -> duration_s"
    ))]
    SecondsBetween,
    #[strum(props(signature = "sub(a: number, b: number) -> number"))]
    Sub,
    #[strum(props(
        signature = "substring(s: string, start: i64 = null, end: i64 = null) -> string"
    ))]
    Substring,
    #[strum(props(
        dfg_signature = "sum(input: number, window: window = null) -> number",
        plan_signature = "sum(input: number, ticks: bool = null, slide_duration: i64 = null) -> \
                          number"
    ))]
    Sum,
    #[strum(props(signature = "time_of(input: any) -> timestamp_ns"))]
    TimeOf,
    #[strum(props(signature = "upper(s: string) -> string"))]
    Upper,
    #[strum(props(
        dfg_signature = "variance(input: number, window: window = null) -> f64",
        plan_signature = "variance(input: number, ticks: bool = null, slide_duration: i64 = null) \
                          -> f64"
    ))]
    Variance,
    #[strum(props(signature = "year(time: timestamp_ns) -> i32"))]
    Year,
    #[strum(props(signature = "zip_max(a: ordered, b: ordered) -> ordered"))]
    ZipMax,
    #[strum(props(signature = "zip_min(a: ordered, b: ordered) -> ordered"))]
    ZipMin,
}

impl InstOp {
    pub fn is_aggregation(&self) -> bool {
        use InstOp::*;
        matches!(
            self,
            Sum | Last | First | CountIf | Min | Max | Mean | Variance
        )
    }

    pub fn signature(&self, mode: Mode) -> &'static Signature {
        match mode {
            Mode::Dfg => INST_OP_SIGNATURES[*self].dfg(),
            Mode::Plan => INST_OP_SIGNATURES[*self].plan(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.signature(Mode::Dfg).name()
    }
}

#[derive(Clone, Debug, PartialEq, Hash, Eq, Ord, PartialOrd)]
pub enum InstKind {
    /// Applies a callable function to the inputs.
    Simple(InstOp),
    /// Accesses the given field of the input.
    FieldRef,
    /// Cast the input to the given datatype.
    Cast(DataType),
    /// Create a record with the given field names.
    ///
    /// The number of arguments should match the number of fields.
    Record,
}

impl fmt::Display for InstKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstKind::Simple(instruction) => write!(f, "{instruction}"),
            InstKind::FieldRef => write!(f, "field"),
            InstKind::Cast(data_type) => write!(f, "cast:{data_type}"),
            InstKind::Record => write!(f, "record"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Inst {
    pub kind: InstKind,
    pub args: Vec<ValueRef>,
}

impl Inst {
    pub fn new(kind: InstKind, args: Vec<ValueRef>) -> Self {
        Inst { kind, args }
    }
}

struct OpSignatures {
    dfg: Arc<Signature>,
    plan: Arc<Signature>,
}

impl OpSignatures {
    pub fn dfg(&self) -> &Signature {
        self.dfg.as_ref()
    }

    pub fn plan(&self) -> &Signature {
        self.plan.as_ref()
    }
}

#[dynamic]
static INST_OP_SIGNATURES: EnumMap<InstOp, OpSignatures> = {
    enum_map! {
        op => {
            let signature = parse_signature(op, "signature");
            let dfg_signature = parse_signature(op, "dfg_signature");
            let plan_signature = parse_signature(op, "plan_signature");

            let signatures = match (signature, dfg_signature, plan_signature) {
                (None, Some(dfg), Some(plan)) => {
                    assert_eq!(dfg.name(), plan.name(), "Names for both DFG and Plan signature must be the same");
                    OpSignatures { dfg, plan }
                },
                (Some(shared), None, None) => {
                    OpSignatures {
                        dfg: shared.clone(),
                        plan: shared
                    }
                }
                (shared, dfg, plan) => {
                    // Must have either (shared) or (dfg and plan).
                    panic!("Missing or invalid signatures for instruction {op:?}: shared={shared:?}, dfg={dfg:?}, plan={plan:?}")
                }
            };

            for parameter in signatures.plan().parameters().types() {
                if matches!(parameter.inner(), FenlType::Window) {
                    panic!("Illegal type '{}' in plan_signature for instruction {:?}", parameter.inner(), op)
                }
            }

            // TODO: Ideally, these would be `InstOp` that just didn't
            // follow the normal rules of having a signature. For now,
            // make sure we don't accidentally introduce `InstOp` with
            // these names (since they are handled specially in the
            // planning / execution).
            assert_ne!(signatures.dfg().name(), "record", "'record' is a reserved instruction name");
            assert_ne!(signatures.dfg().name(), "field_ref", "'field_ref' is a reserved instruction name");

            signatures
        }
    }
};

fn parse_signature(op: InstOp, label: &'static str) -> Option<Arc<Signature>> {
    op.get_str(label).map(|signature_str| {
        let signature =
            Signature::try_from_str(FeatureSetPart::Internal(signature_str), signature_str)
                .unwrap_or_else(|e| {
                    panic!("Invalid {label} '{signature_str}' for instruction {op:?}: {e:?}")
                });

        if signature.name() != op.to_string() {
            panic!("Signature for op '{op:?}' has invalid name: '{signature_str}'")
        }
        Arc::new(signature)
    })
}
