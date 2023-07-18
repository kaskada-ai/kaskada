use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::{DataType, TimeUnit};
use itertools::Itertools;
use sparrow_arrow::scalar_value::{
    timeunit_from_suffix, timeunit_suffix, ScalarRecord, ScalarTimestamp, ScalarValue,
};

use super::{expression_plan, operation_plan, OperationInputRef};
use crate::kaskada::v1alpha::{literal, Literal};
const TIMESTAMP: DataType = DataType::Timestamp(TimeUnit::Nanosecond, None);

impl crate::kaskada::v1alpha::LateBoundValue {
    pub fn data_type(&self) -> anyhow::Result<&'static DataType> {
        match self {
            Self::Unspecified => Err(anyhow::anyhow!(
                "No data type for LateBoundValue::Unspecified"
            )),
            Self::ChangedSinceTime => Ok(&TIMESTAMP),
            Self::FinalAtTime => Ok(&TIMESTAMP),
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified_late_bound",
            Self::ChangedSinceTime => "changed_since_time",
            Self::FinalAtTime => "final_at_time",
        }
    }
}

// TODO: Move conversions to ScalarValue to simplify package dependency.
// sparrow-api depends on sparrow-syntax to implement type conversion.
impl Literal {
    pub fn try_into_scalar_value(&self, data_type: &DataType) -> anyhow::Result<ScalarValue> {
        let value = match &self.literal {
            Some(literal::Literal::Bool(v)) => ScalarValue::Boolean(Some(*v)),
            Some(literal::Literal::Int8(v)) => ScalarValue::Int8(Some(*v as i8)),
            Some(literal::Literal::Int16(v)) => ScalarValue::Int16(Some(*v as i16)),
            Some(literal::Literal::Int32(v)) => ScalarValue::Int32(Some(*v)),
            Some(literal::Literal::Int64(v)) => ScalarValue::Int64(Some(*v)),
            Some(literal::Literal::Uint8(v)) => ScalarValue::UInt8(Some(*v as u8)),
            Some(literal::Literal::Uint16(v)) => ScalarValue::UInt16(Some(*v as u16)),
            Some(literal::Literal::Uint32(v)) => ScalarValue::UInt32(Some(*v)),
            Some(literal::Literal::Uint64(v)) => ScalarValue::UInt64(Some(*v)),
            Some(literal::Literal::Float32(v)) => ScalarValue::Float32(Some((*v).into())),
            Some(literal::Literal::Float64(v)) => ScalarValue::Float64(Some((*v).into())),
            Some(literal::Literal::Timestamp(v)) => {
                let tu = timeunit_from_suffix(&v.unit)?;
                let tz = v.tz.as_ref().map(|tz| Arc::from(tz.clone()));
                ScalarValue::Timestamp(Box::new(ScalarTimestamp::new(v.value, tu, tz)))
            }
            Some(literal::Literal::Date32(v)) => ScalarValue::Date32(Some(*v)),
            Some(literal::Literal::Date64(v)) => ScalarValue::Date64(Some(*v)),
            Some(literal::Literal::Time32(v)) => {
                let tu = timeunit_from_suffix(&v.unit)?;
                ScalarValue::Time32(Some(v.value), tu)
            }
            Some(literal::Literal::Time64(v)) => {
                let tu = timeunit_from_suffix(&v.unit)?;
                ScalarValue::Time64(Some(v.value), tu)
            }
            Some(literal::Literal::Duration(v)) => {
                let tu = timeunit_from_suffix(&v.unit)?;
                ScalarValue::Duration(Some(v.value), tu)
            }
            Some(literal::Literal::IntervalDayTime(v)) => {
                ScalarValue::IntervalDayTime(Some((v.start, v.end)))
            }
            Some(literal::Literal::IntervalMonths(v)) => ScalarValue::IntervalMonths(Some(*v)),
            Some(literal::Literal::Utf8(v)) => ScalarValue::Utf8(Some(v.clone())),
            Some(literal::Literal::LargeUtf8(v)) => ScalarValue::LargeUtf8(Some(v.clone())),
            Some(literal::Literal::Record(v)) => {
                let values = v
                    .values
                    .iter()
                    .map(|v| v.clone().try_into_scalar_value(data_type))
                    .collect::<anyhow::Result<Vec<ScalarValue>>>()?;

                let fields = if let DataType::Struct(fields) = data_type {
                    fields
                } else {
                    unreachable!("Record value has non-struct type {:?}", data_type)
                };

                ScalarValue::Record(Box::new(ScalarRecord::new(Some(values), fields.clone())))
            }
            None => ScalarValue::try_new_null(data_type)?,
        };
        anyhow::ensure!(&value.data_type() == data_type);
        Ok(value)
    }
}

impl From<&ScalarValue> for Literal {
    fn from(scalar: &ScalarValue) -> Self {
        let literal = match scalar {
            ScalarValue::Boolean(Some(v)) => Some(literal::Literal::Bool(*v)),
            ScalarValue::Int8(Some(v)) => Some(literal::Literal::Int8(*v as i32)),
            ScalarValue::Int16(Some(v)) => Some(literal::Literal::Int16(*v as i32)),
            ScalarValue::Int32(Some(v)) => Some(literal::Literal::Int32(*v)),
            ScalarValue::Int64(Some(v)) => Some(literal::Literal::Int64(*v)),
            ScalarValue::UInt8(Some(v)) => Some(literal::Literal::Uint8(*v as u32)),
            ScalarValue::UInt16(Some(v)) => Some(literal::Literal::Uint16(*v as u32)),
            ScalarValue::UInt32(Some(v)) => Some(literal::Literal::Uint32(*v)),
            ScalarValue::UInt64(Some(v)) => Some(literal::Literal::Uint64(*v)),
            ScalarValue::Float32(Some(v)) => Some(literal::Literal::Float32(v.into_inner())),
            ScalarValue::Float64(Some(v)) => Some(literal::Literal::Float64(v.into_inner())),
            ScalarValue::Timestamp(v) => {
                let tz = v.tz().map(|tz| tz.as_ref().to_owned());
                Some(literal::Literal::Timestamp(literal::TimestampValue {
                    value: v.value(),
                    unit: timeunit_suffix(&v.unit()).to_string(),
                    tz,
                }))
            }
            ScalarValue::Time64(Some(v), u) => {
                Some(literal::Literal::Time64(literal::Time64Value {
                    value: *v,
                    unit: timeunit_suffix(u).to_string(),
                }))
            }
            ScalarValue::Duration(Some(v), u) => {
                Some(literal::Literal::Duration(literal::DurationValue {
                    value: *v,
                    unit: timeunit_suffix(u).to_string(),
                }))
            }
            ScalarValue::IntervalDayTime(Some((l, r))) => Some(literal::Literal::IntervalDayTime(
                literal::IntervalDayTimeValue { start: *l, end: *r },
            )),
            ScalarValue::IntervalMonths(Some(v)) => Some(literal::Literal::IntervalMonths(*v)),
            ScalarValue::Utf8(Some(v)) => Some(literal::Literal::Utf8(v.clone())),
            ScalarValue::LargeUtf8(Some(v)) => Some(literal::Literal::LargeUtf8(v.clone())),
            ScalarValue::Record(v) => {
                let values: Vec<_> = v
                    .values()
                    .as_ref()
                    .map_or(vec![], |vs| vs.iter().map(Self::from).collect::<Vec<_>>());

                Some(literal::Literal::Record(literal::RecordValue { values }))
            }
            // This covers both the explicit `ScalarValue::Null` case and the many variants of
            // `ScalarValue::Something(None)`.
            _ => None,
        };

        Literal { literal }
    }
}

impl super::ComputePlan {
    pub fn write_to_graphviz_path(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(file);

        self.write_to_graphviz(&mut writer)
    }

    pub fn write_to_graphviz(&self, w: &mut impl std::io::Write) -> anyhow::Result<()> {
        writeln!(w, "digraph plan {{")?;

        // Styling.
        writeln!(w, "  rankdir=BT;")?;
        writeln!(w, "  node[shape=plain, ordering=in];")?;

        for (index, operation) in self.operations.iter().enumerate() {
            operation.write(index, w)?;
        }

        writeln!(w, "}}")?;

        Ok(())
    }
}

impl super::OperationPlan {
    pub fn operator(&self) -> anyhow::Result<&operation_plan::Operator> {
        self.operator.as_ref().context("missing operator")
    }

    fn write(&self, op_index: usize, w: &mut impl std::io::Write) -> anyhow::Result<()> {
        // Expressions are potentially unboundedly large, and their YAML is present
        // on the corresponding expression row. So we don't include them in the YAML
        // for the operation.
        let mut without_expressions = self.clone();
        without_expressions.expressions.clear();
        let operation_yaml = escaped_yaml(&without_expressions)?;

        writeln!(w, "  operation_{op_index} [label=<")?;
        writeln!(w, "    <table>")?;
        writeln!(
            w,
            // Graphviz only renders tooltips on HTML labels if they contain a `href`.
            "      <tr><td colspan=\"3\" href=\".\" tooltip=\"{operation_yaml}\"><b>Operation \
             {op_index}: {}</b></td></tr>",
            OperationName(self)
        )?;
        writeln!(
            w,
            "      <tr><td>Index</td><td>Operator</td><td>Inputs</td></tr>",
        )?;

        for (expr_index, expr) in self.expressions.iter().enumerate() {
            let cell_style = if expr.output { " bgcolor=\"gold\"" } else { "" };
            let expression_yaml = escaped_yaml(expr)?;

            // Graphviz only renders tooltips on HTML labels if they contain a `href`.
            writeln!(
                w,
                "      <tr><td{cell_style}>{expr_index}</td><td{cell_style} href=\".\" \
                 tooltip=\"{expression_yaml}\">{}</td><td{cell_style}>{}</td></tr>",
                expr.operator.as_ref().pretty_fmt(),
                expr.arguments.iter().format(", ")
            )?;
        }

        writeln!(w, "    </table>")?;
        writeln!(w, "  >];")?;

        writeln!(
            w,
            "  {{{}}} -> operation_{op_index};\n",
            self.operator()?
                .input_ops_iter()
                .filter(|elt| *elt as usize != op_index)
                .format_with(",", |elt, f| f(&format_args!("operation_{elt}"))),
        )?;

        Ok(())
    }
}

#[repr(transparent)]
struct OperationName<'a>(&'a super::OperationPlan);

impl<'a> std::fmt::Display for OperationName<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.operator.as_ref().pretty_fmt())
    }
}

#[repr(transparent)]
struct PrettyFmt<'a, T: Pretty + ?Sized>(&'a T);

impl<'a, T: Pretty> std::fmt::Display for PrettyFmt<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.pretty(f)
    }
}

trait Pretty {
    /// Pretty formatting for the given type.
    fn pretty(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

    /// Returns a struct which can be used for the pretty format.
    fn pretty_fmt(&self) -> PrettyFmt<'_, Self> {
        PrettyFmt(self)
    }
}

impl<'a, T: Pretty> Pretty for Option<&'a T> {
    fn pretty(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            None => write!(f, "???"),
            Some(item) => item.pretty(f),
        }
    }
}

impl Pretty for OperationInputRef {
    fn pretty(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::kaskada::v1alpha::operation_input_ref::{Column, KeyColumn};
        let operation = self.producing_operation;
        match &self.column {
            Some(Column::KeyColumn(key_column)) => {
                let key_column = match KeyColumn::from_i32(*key_column) {
                    Some(KeyColumn::Unspecified) => "unspecified_key",
                    Some(KeyColumn::Time) => "time",
                    Some(KeyColumn::Subsort) => "subsort",
                    Some(KeyColumn::KeyHash) => "key_hash",
                    None => "unknown_key",
                };
                write!(f, "{operation}.{key_column}")
            }
            Some(Column::ProducerExpression(expression)) => {
                write!(f, "{operation}.{expression}")
            }
            Some(Column::ScanRecord(())) => {
                write!(f, "{operation}.input")
            }
            Some(Column::Tick(())) => {
                write!(f, "{operation}.tick")
            }
            None => write!(f, "{operation}.missing"),
        }
    }
}

impl Pretty for expression_plan::Operator {
    fn pretty(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            expression_plan::Operator::Instruction(inst) => write!(f, "Instruction '{inst}'"),
            expression_plan::Operator::Input(input) => {
                write!(f, "Input from {}", input.pretty_fmt())
            }
            expression_plan::Operator::Literal(literal) => write!(f, "Literal {literal}"),
            expression_plan::Operator::LateBound(late_bound) => {
                write!(f, "LateBound {late_bound:?}")
            }
        }
    }
}

impl Pretty for operation_plan::Operator {
    fn pretty(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            operation_plan::Operator::Scan(scan) => {
                write!(
                    f,
                    "Scan '{}'",
                    scan.slice_plan
                        .as_ref()
                        .map(|slice| &slice.table_name)
                        .pretty_fmt()
                )
            }
            operation_plan::Operator::Select(select) => {
                write!(f, "Select ({})", select.condition.as_ref().pretty_fmt())
            }
            operation_plan::Operator::Merge(merge) => {
                write!(f, "Merge ({}, {})", merge.left, merge.right)
            }
            operation_plan::Operator::Tick(tick) => {
                write!(f, "Tick ({})", tick.behavior())
            }
            operation_plan::Operator::WithKey(with_key) => {
                write!(
                    f,
                    "WithKey (new_key={})",
                    with_key.new_key.as_ref().pretty_fmt(),
                )
            }
            operation_plan::Operator::LookupRequest(lookup_request) => {
                write!(
                    f,
                    "LookupRequest (foreign_key_hash={})",
                    lookup_request.foreign_key_hash.as_ref().pretty_fmt(),
                )
            }
            operation_plan::Operator::LookupResponse(lookup_response) => {
                write!(
                    f,
                    "LookupResponse (requesting_key_hash={})",
                    lookup_response.requesting_key_hash.as_ref().pretty_fmt(),
                )
            }
            operation_plan::Operator::ShiftTo(shift) => match &shift.time {
                Some(operation_plan::shift_to_operation::Time::Computed(input)) => {
                    write!(f, "ShiftTo (time={})", input.pretty_fmt())
                }
                Some(operation_plan::shift_to_operation::Time::Literal(timestamp)) => {
                    write!(f, "ShiftTo (literal={timestamp:?})")
                }
                None => write!(f, "ShiftTo (???)"),
            },
            operation_plan::Operator::ShiftUntil(shift) => {
                write!(
                    f,
                    "ShiftUntil (condition={})",
                    shift.condition.as_ref().pretty_fmt()
                )
            }
        }
    }
}

impl Pretty for String {
    fn pretty(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for super::Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.literal {
            None => write!(f, "null"),
            Some(literal::Literal::Bool(true)) => write!(f, "true"),
            Some(literal::Literal::Bool(false)) => write!(f, "false"),
            Some(literal::Literal::Int8(n)) => write!(f, "{n}i8"),
            Some(literal::Literal::Int16(n)) => write!(f, "{n}i16"),
            Some(literal::Literal::Int32(n)) => write!(f, "{n}i32"),
            Some(literal::Literal::Int64(n)) => write!(f, "{n}i64"),
            Some(literal::Literal::Uint8(n)) => write!(f, "{n}u8"),
            Some(literal::Literal::Uint16(n)) => write!(f, "{n}u16"),
            Some(literal::Literal::Uint32(n)) => write!(f, "{n}u32"),
            Some(literal::Literal::Uint64(n)) => write!(f, "{n}u64"),
            Some(literal::Literal::Float32(n)) => write!(f, "{n}f32"),
            Some(literal::Literal::Float64(n)) => write!(f, "{n}f64"),
            Some(literal::Literal::Timestamp(ref timestamp)) => {
                assert!(
                    timestamp.tz.is_none(),
                    "Timestamps with time zones not yet supported"
                );
                let value = timestamp.value.expect("null handled above");
                write!(f, "timestamp_{}:{}", timestamp.unit, value)
            }
            Some(literal::Literal::Date32(n)) => write!(f, "date32:{n}"),
            Some(literal::Literal::Date64(n)) => write!(f, "date64:{n}"),
            Some(literal::Literal::Time32(ref t)) => {
                write!(f, "time32_{}:{}", t.unit, t.value)
            }
            Some(literal::Literal::Time64(ref t)) => {
                write!(f, "time64_{}:{}", t.unit, t.value)
            }
            Some(literal::Literal::Duration(ref d)) => {
                write!(f, "duration_{}:{}", d.unit, d.value)
            }
            Some(literal::Literal::IntervalDayTime(ref i)) => {
                write!(f, "interval_days:{},{}", i.start, i.end)
            }
            Some(literal::Literal::IntervalMonths(i)) => {
                write!(f, "interval_months:{i}")
            }
            Some(literal::Literal::Utf8(str)) => write!(f, "\\\"{str}\\\""),
            Some(literal::Literal::LargeUtf8(str)) => write!(f, "\\\"{str}\\\""),
            unreachable => unreachable!("Unable to format {:?}", unreachable),
        }
    }
}

impl operation_plan::Operator {
    pub fn label(&self) -> &'static str {
        match self {
            operation_plan::Operator::Scan(_) => "scan",
            operation_plan::Operator::Merge(_) => "merge",
            operation_plan::Operator::Select(_) => "select",
            operation_plan::Operator::WithKey(_) => "with_key",
            operation_plan::Operator::Tick(_) => "tick",
            operation_plan::Operator::LookupRequest(_) => "lookup_request",
            operation_plan::Operator::LookupResponse(_) => "lookup_response",
            operation_plan::Operator::ShiftTo(_) => "shift_to",
            operation_plan::Operator::ShiftUntil(_) => "shift_until",
        }
    }

    pub fn input_len(&self) -> usize {
        match self {
            operation_plan::Operator::Scan(_) => 0,
            operation_plan::Operator::Merge(_) => 2,
            operation_plan::Operator::Select(_) => 1,
            operation_plan::Operator::WithKey(_) => 1,
            operation_plan::Operator::Tick(_) => 1,
            operation_plan::Operator::LookupRequest(_) => 1,
            operation_plan::Operator::LookupResponse(_) => 1,
            operation_plan::Operator::ShiftTo(_) => 1,
            operation_plan::Operator::ShiftUntil(_) => 1,
        }
    }

    pub fn input_op(&self, index: usize) -> anyhow::Result<u32> {
        match (self, index) {
            (operation_plan::Operator::Merge(merge), 0) => Ok(merge.left),
            (operation_plan::Operator::Merge(merge), 1) => Ok(merge.right),
            (operation_plan::Operator::Select(select), 0) => Ok(select.input),
            (operation_plan::Operator::WithKey(with_key), 0) => Ok(with_key.input),
            (operation_plan::Operator::LookupRequest(lookup_request), 0) => {
                Ok(lookup_request.primary_operation)
            }
            (operation_plan::Operator::LookupResponse(lookup_response), 0) => {
                Ok(lookup_response.foreign_operation)
            }
            (operation_plan::Operator::Tick(tick), 0) => Ok(tick.input),
            (operation_plan::Operator::ShiftTo(shift_to), 0) => Ok(shift_to.input),
            (operation_plan::Operator::ShiftUntil(shift_until), 0) => Ok(shift_until.input),
            (op, index) => Err(anyhow::anyhow!(
                "Index {index} out-of-bounds. Operator {op:?} only has {} inputs.",
                op.input_len()
            )),
        }
    }

    pub fn input_op_mut(&mut self, index: usize) -> anyhow::Result<&mut u32> {
        match (self, index) {
            (operation_plan::Operator::Merge(merge), 0) => Ok(&mut merge.left),
            (operation_plan::Operator::Merge(merge), 1) => Ok(&mut merge.right),
            (operation_plan::Operator::Select(select), 0) => Ok(&mut select.input),
            (operation_plan::Operator::LookupRequest(lookup_request), 0) => {
                Ok(&mut lookup_request.primary_operation)
            }
            (operation_plan::Operator::LookupResponse(lookup_response), 0) => {
                Ok(&mut lookup_response.foreign_operation)
            }
            (operation_plan::Operator::Tick(tick), 0) => Ok(&mut tick.input),
            (operation_plan::Operator::ShiftTo(shift_to), 0) => Ok(&mut shift_to.input),
            (operation_plan::Operator::ShiftUntil(shift_until), 0) => Ok(&mut shift_until.input),
            (op, index) => Err(anyhow::anyhow!(
                "Index {index} out-of-bounds. Operator {op:?} only has {} inputs.",
                op.input_len()
            )),
        }
    }

    pub fn input_ops_iter(&self) -> impl Iterator<Item = u32> + '_ {
        // Unwrap is mostly safe -- we iterate up to the length only, so this
        // should only panic if `input_len()` and `input()` disagree.
        (0..self.input_len()).map(|index| self.input_op(index).unwrap())
    }

    pub fn operation_input_ref_len(&self) -> usize {
        match self {
            operation_plan::Operator::WithKey(_) => 1,
            operation_plan::Operator::Scan(_) => 0,
            operation_plan::Operator::Merge(_) => 0,
            operation_plan::Operator::Select(_) => 1,
            operation_plan::Operator::Tick(_) => 0,
            operation_plan::Operator::LookupRequest(_) => 1,
            operation_plan::Operator::LookupResponse(_) => 1,
            operation_plan::Operator::ShiftTo(shift) => match &shift.time {
                Some(operation_plan::shift_to_operation::Time::Computed(_)) => 1,
                Some(operation_plan::shift_to_operation::Time::Literal(_)) => 0,
                None => 0,
            },
            operation_plan::Operator::ShiftUntil(_) => 1,
        }
    }

    pub fn operation_input_ref(&self, index: usize) -> anyhow::Result<&OperationInputRef> {
        match (self, index) {
            (operation_plan::Operator::WithKey(with_key), 0) => with_key
                .new_key
                .as_ref()
                .context("missing operation input ref"),
            (operation_plan::Operator::Select(select), 0) => select
                .condition
                .as_ref()
                .context("missing operation input ref"),
            (operation_plan::Operator::LookupRequest(request), 0) => request
                .foreign_key_hash
                .as_ref()
                .context("missing operation input ref"),
            (operation_plan::Operator::LookupResponse(response), 0) => response
                .requesting_key_hash
                .as_ref()
                .context("missing operation input ref"),
            (_op @ operation_plan::Operator::ShiftTo(shift), 0) => match &shift.time {
                Some(operation_plan::shift_to_operation::Time::Computed(computed)) => Ok(computed),
                _ => Err(anyhow::anyhow!("missing operation input ref")),
            },
            (operation_plan::Operator::ShiftUntil(shift), 0) => shift
                .condition
                .as_ref()
                .context("missing operation input ref"),
            (op, index) => Err(anyhow::anyhow!(
                "Index {index} out-of-bounds. Operator {op:?} only has {} op input refs.",
                op.input_len()
            )),
        }
    }

    pub fn operation_input_ref_mut(
        &mut self,
        index: usize,
    ) -> anyhow::Result<&mut OperationInputRef> {
        match (self, index) {
            (operation_plan::Operator::WithKey(with_key), 0) => with_key
                .new_key
                .as_mut()
                .context("missing operation input ref"),
            (operation_plan::Operator::Select(select), 0) => select
                .condition
                .as_mut()
                .context("missing operation input ref"),
            (operation_plan::Operator::LookupRequest(request), 0) => request
                .foreign_key_hash
                .as_mut()
                .context("missing operation input ref"),
            (operation_plan::Operator::LookupResponse(response), 0) => response
                .requesting_key_hash
                .as_mut()
                .context("missing operation input ref"),
            (operation_plan::Operator::ShiftTo(shift), 0) => match &mut shift.time {
                Some(operation_plan::shift_to_operation::Time::Computed(computed)) => Ok(computed),
                _ => Err(anyhow::anyhow!(
                    "Index 0 out-of-bounds. ShiftTo only has 0 op input refs.",
                )),
            },
            (operation_plan::Operator::ShiftUntil(shift), 0) => shift
                .condition
                .as_mut()
                .context("missing operation input ref"),
            (op, index) => Err(anyhow::anyhow!(
                "Index {index} out-of-bounds. Operator {op:?} only has {} op input refs.",
                op.input_len()
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct Escaped {
    state: EscapedState,
}

impl Escaped {
    fn new(c: char) -> Escaped {
        let init_state = match c {
            '\t' => EscapedState::String("\\t".chars()),
            '\n' => EscapedState::String("&#13;".chars()),
            '\r' => EscapedState::String("\\r".chars()),
            '\\' => EscapedState::String("\\\\".chars()),
            '"' => EscapedState::String("&#34;".chars()),
            '<' => EscapedState::String("&#lt;".chars()),
            '>' => EscapedState::String("&#gt;".chars()),
            '\x20'..='\x7e' => EscapedState::Char(c),
            _ => EscapedState::Unicode(c.escape_unicode()),
        };
        Escaped { state: init_state }
    }
}

#[derive(Clone, Debug)]
enum EscapedState {
    Char(char),
    String(std::str::Chars<'static>),
    Unicode(std::char::EscapeUnicode),
}

impl Iterator for Escaped {
    type Item = char;

    fn next(&mut self) -> Option<char> {
        match self.state {
            EscapedState::Char(c) => {
                self.state = EscapedState::String("".chars());
                Some(c)
            }
            EscapedState::String(ref mut iter) => iter.next(),
            EscapedState::Unicode(ref mut iter) => iter.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.len();
        (n, Some(n))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }
}

impl ExactSizeIterator for Escaped {
    fn len(&self) -> usize {
        match &self.state {
            EscapedState::Char(_) => 1,
            EscapedState::String(s) => s.as_str().len(),
            EscapedState::Unicode(iter) => iter.len(),
        }
    }
}

impl std::iter::FusedIterator for Escaped {}

fn escaped_yaml<D: serde::Serialize>(label: &D) -> anyhow::Result<String> {
    let label = serde_yaml::to_string(label)?;
    Ok(label.chars().flat_map(Escaped::new).collect())
}
