use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::DataType;
use enum_map::{enum_map, Enum, EnumMap};
use parse_display::{Display, FromStr};
use sparrow_syntax::{FeatureSetPart, FenlType, Signature};
use static_init::dynamic;
use strum::EnumProperty;

use crate::{value::ValueRef, Udf};

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
    strum_macros::EnumDiscriminants,
    Eq,
    FromStr,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[display(style = "snake_case")]
pub enum InstOp {
    #[strum(props(signature = "add<N: number>(a: N, b: N) -> N"))]
    Add,
    #[strum(props(
        signature = "add_time<D: timedelta>(delta: D, time: timestamp_ns) -> timestamp_ns"
    ))]
    AddTime,
    #[strum(props(signature = "ceil<N: number>(n: N) -> N"))]
    Ceil,
    #[strum(props(signature = "clamp<N: number>(value: N, min: N = null, max: N = null) -> N"))]
    Clamp,
    #[strum(props(signature = "coalesce<T: any>(values+: T) -> T"))]
    Coalesce,
    #[strum(props(
        signature = "collect<T: any>(input: T, const max: i64, const min: i64 = 0, ticks: bool = null, slide_duration: i64 = null) -> list<T>"
    ))]
    Collect,
    #[strum(props(
        signature = "count_if<T: any>(input: T, ticks: bool = null, slide_duration: i64 = null) -> \
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
    #[strum(props(signature = "div<N: number>(a: N, b: N) -> N"))]
    Div,
    #[strum(props(signature = "eq<T: any>(a: T, b: T) -> bool"))]
    Eq,
    #[strum(props(signature = "exp(power: f64) -> f64"))]
    Exp,
    #[strum(props(
        signature = "first<T: any>(input: T, ticks: bool = null, slide_duration: i64 = null) -> T"
    ))]
    First,
    #[strum(props(signature = "flatten<T: any>(input: list<list<T>>) -> list<T>"))]
    Flatten,
    #[strum(props(signature = "floor<N: number>(n: N) -> N"))]
    Floor,
    #[strum(props(signature = "get<K: key, V: any>(key: K, map: map<K, V>) -> V"))]
    Get,
    #[strum(props(signature = "gt<O: ordered>(a: O, b: O) -> bool"))]
    Gt,
    #[strum(props(signature = "gte<O: ordered>(a: O, b: O) -> bool"))]
    Gte,
    #[strum(props(signature = "hash<T: any>(input: T) -> u64"))]
    Hash,
    #[strum(props(signature = "if<T: any>(condition: bool, value: T) -> T"))]
    If,
    #[strum(props(signature = "index<T: any>(i: i64, list: list<T>) -> T"))]
    Index,
    #[strum(props(signature = "is_valid<T: any>(input: T) -> bool"))]
    IsValid,
    // HACK: This instruction does not show up in the plan/does not have an evaluator.
    // It is converted to `JsonField` during simplification.
    #[strum(props(signature = "json(s: string) -> json"))]
    Json,
    #[strum(props(signature = "json_field(s: string, field: string) -> string"))]
    JsonField,
    #[strum(props(
        signature = "last<T: any>(input: T, ticks: bool = null, slide_duration: i64 = null) -> T"
    ))]
    Last,
    #[strum(props(signature = "len(s: string) -> i32"))]
    Len,
    #[strum(props(signature = "list_len<T: any>(input: list<T>) -> i32"))]
    ListLen,
    #[strum(props(signature = "logical_and(a: bool, b: bool) -> bool"))]
    LogicalAnd,
    #[strum(props(signature = "logical_or(a: bool, b: bool) -> bool"))]
    LogicalOr,
    #[strum(props(signature = "lower(s: string) -> string"))]
    Lower,
    #[strum(props(signature = "lt<O: ordered>(a: O, b: O) -> bool"))]
    Lt,
    #[strum(props(signature = "lte<O: ordered>(a: O, b: O) -> bool"))]
    Lte,
    #[strum(props(
        signature = "max<O: ordered>(input: O, ticks: bool = null, slide_duration: i64 = null) -> O"
    ))]
    Max,
    #[strum(props(
        signature = "mean<N: number>(input: N, ticks: bool = null, slide_duration: i64 = null) -> \
                          f64"
    ))]
    Mean,
    #[strum(props(
        signature = "min<O: ordered>(input: O, ticks: bool = null, slide_duration: i64 = null) -> O"
    ))]
    Min,
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
    #[strum(props(signature = "mul<N: number>(a: N, b: N) -> N"))]
    Mul,
    #[strum(props(signature = "neg<S: signed>(n: S) -> S"))]
    Neg,
    #[strum(props(signature = "neq<T: any>(a: T, b: T) -> bool"))]
    Neq,
    #[strum(props(signature = "not(input: bool) -> bool"))]
    Not,
    #[strum(props(signature = "null_if<T: any>(condition: bool, value: T) -> T"))]
    NullIf,
    #[strum(props(signature = "powf(base: f64, power: f64) -> f64"))]
    Powf,
    #[strum(props(signature = "round<N: number>(n: N) -> N"))]
    Round,
    #[strum(props(signature = "seconds(seconds: i64) -> duration_s"))]
    Seconds,
    #[strum(props(
        signature = "seconds_between(t1: timestamp_ns, t2: timestamp_ns) -> duration_s"
    ))]
    SecondsBetween,
    #[strum(props(signature = "sub<N: number>(a: N, b: N) -> N"))]
    Sub,
    #[strum(props(
        signature = "substring(s: string, start: i64 = null, end: i64 = null) -> string"
    ))]
    Substring,
    #[strum(props(
        signature = "sum<N: number>(input: N, ticks: bool = null, slide_duration: i64 = null) -> \
                          N"
    ))]
    Sum,
    #[strum(props(signature = "time_of<T: any>(input: T) -> timestamp_ns"))]
    TimeOf,
    #[strum(props(signature = "upper(s: string) -> string"))]
    Upper,
    #[strum(props(signature = "union<T: any>(a: list<T>, b: list<T>) -> list<T>"))]
    Union,
    #[strum(props(
        signature = "variance<N: number>(input: N, ticks: bool = null, slide_duration: i64 = null) \
                          -> f64"
    ))]
    Variance,
    #[strum(props(signature = "year(time: timestamp_ns) -> i32"))]
    Year,
    #[strum(props(signature = "zip_max<O: ordered>(a: O, b: O) -> O"))]
    ZipMax,
    #[strum(props(signature = "zip_min<O: ordered>(a: O, b: O) -> O"))]
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

    pub fn signature(&self) -> &'static Signature {
        &INST_OP_SIGNATURES[*self]
    }

    pub fn name(&self) -> &'static str {
        self.signature().name()
    }
}

// #[derive(Clone, Debug, PartialEq, Hash, Eq, Ord, PartialOrd)]
#[derive(Debug, Eq, Ord, PartialOrd)]
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
    /// A user defined function.
    Udf(Arc<dyn Udf>),
}

impl Clone for InstKind {
    fn clone(&self) -> Self {
        match self {
            Self::Simple(arg0) => Self::Simple(*arg0),
            Self::FieldRef => Self::FieldRef,
            Self::Cast(arg0) => Self::Cast(arg0.clone()),
            Self::Record => Self::Record,
            Self::Udf(arg0) => Self::Udf(arg0.clone()),
        }
    }
}

impl PartialEq for InstKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Simple(l0), Self::Simple(r0)) => l0 == r0,
            (Self::FieldRef, Self::FieldRef) => true,
            (Self::Cast(l0), Self::Cast(r0)) => l0 == r0,
            (Self::Record, Self::Record) => true,
            (Self::Udf(l0), Self::Udf(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Hash for InstKind {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            InstKind::Udf(udf) => udf.hash(state),
            InstKind::Simple(op) => op.hash(state),
            InstKind::Cast(dt) => dt.hash(state),
            InstKind::Record => {}
            InstKind::FieldRef => {}
        }
    }
}

impl fmt::Display for InstKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstKind::Simple(instruction) => write!(f, "{instruction}"),
            InstKind::FieldRef => write!(f, "field"),
            InstKind::Cast(data_type) => write!(f, "cast:{data_type}"),
            InstKind::Record => write!(f, "record"),
            InstKind::Udf(udf) => write!(f, "{}", udf.signature().name()),
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

#[dynamic]
static INST_OP_SIGNATURES: EnumMap<InstOp, Arc<Signature>> = {
    enum_map! {
        op => {
            let signature = parse_signature(op, "signature");
            let signature = match signature {
                Some(s) => s,
                None => panic!("Missing or invalid signatures for instruction {op:?}: signature={signature:?}")
            };

            for parameter in signature.parameters().types() {
                if matches!(parameter.inner(), FenlType::Window) {
                    panic!("Illegal type '{}' in signature for instruction {:?}", parameter.inner(), op)
                }
            }

            // TODO: Ideally, these would be `InstOp` that just didn't
            // follow the normal rules of having a signature. For now,
            // make sure we don't accidentally introduce `InstOp` with
            // these names (since they are handled specially in the
            // planning / execution).
            assert_ne!(signature.name(), "record", "'record' is a reserved instruction name");
            assert_ne!(signature.name(), "field_ref", "'field_ref' is a reserved instruction name");

            signature
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
