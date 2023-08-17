// TODO: Intern symbols in the step kind?

use std::fmt;
use std::str::FromStr;

use anyhow::Context;
use sparrow_api::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;
use sparrow_api::kaskada::v1alpha::{slice_plan, LateBoundValue};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::{InstKind, InstOp, TableId};
use sparrow_syntax::WindowBehavior;
use uuid::Uuid;

/// The operation nodes in the DFG.
///
/// These correspond to nodes that define the domain of an expression.
///
/// Each operation takes a number of arguments as described.
#[derive(Debug, Clone, PartialEq, Hash, Eq, PartialOrd, Ord, strum_macros::IntoStaticStr)]
pub(crate) enum Operation {
    /// Operation describing the empty domain.
    ///
    /// This is generally used for literals and expressions applied only to
    /// literals. Before use with as part of an expression should be transformed
    /// to the appropriate domain.
    Empty,
    /// Operation which scans a table. The domain is the content of the table.
    Scan {
        table_id: TableId,
        slice: Option<slice_plan::Slice>,
    },
    /// Operation which performs a sorted merge-join.
    ///
    /// Takes two arguments from possibly different operations and merges them.
    MergeJoin,
    /// A lookup request.
    ///
    /// Takes two arguments
    /// - The expression computing the foreign key to request.
    /// - The operation containing the foreign value.
    LookupRequest,
    /// A lookup response.
    ///
    /// Takes two arguments:
    /// - The lookup request operation being responded to.
    /// - The foreign operation which can access both the lookup request and the
    ///   foreign value.
    LookupResponse,
    /// Operation computing new keys for an expression.
    ///
    /// Takes two arguments from the same operation -- the expression computing
    /// the key and the expression for the value to be re-keyed.
    WithKey,
    /// The `select` (filter) operation.
    ///
    /// Takes one argument corresponding to the condition. Defines a new
    /// operation corresponding to the rows in which the condition is true.
    /// Other values from the same operation as the condition should be
    /// `transformed` to the `select` operation.
    Select,
    /// The `shift_to` operation.
    ///
    /// Takes one argument corresponding to the time to shift to, which may be a
    /// literal or a node computing a time. Defines a new operation which
    /// other values from the same operation as the time may be transformed
    /// to.
    ShiftTo,
    /// The `shift_until` operation.
    ///
    /// Takes one argument corresponding to the predicate which determines
    /// whether to output. Defines a new operation which other values from the
    /// same operation as the time may be transformed to.
    ShiftUntil,
    /// Takes one operation argument indicating the operation to tick over.
    ///
    /// Used for creating signals at periodic points in time.
    Tick(TickBehavior),
}

/// The expression nodes in the DFG.
///
/// Every expression takes at least one argument corresponding to the operation
/// the expression is in.
#[derive(Debug, Clone, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub(crate) enum Expression {
    /// A literal with the given value.
    Literal(ScalarValue),
    /// A late bound value.
    LateBound(LateBoundValue),
    /// An instruction executing within an operation.
    Inst(InstKind),
}

#[derive(Debug, Clone, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub(crate) enum StepKind {
    /// An operation defining the domain (rows) of results.
    Operation(Operation),
    /// An expression executing within a given operation.
    ///
    /// All inputs to an expression should be in the same domain, and
    /// the final argument to the expression should be the operation
    /// defining that domain.
    ///
    /// Simplification rules should generally avoid changing the operation
    /// associated with an expression.
    Expression(Expression),
    /// `(transform e op)` changes the domain of `e` to the domain produced by
    /// `op`.
    ///
    /// The operation of `op` must be derived from the domain of `e` (for
    /// instance the resulting of merging it and another domain, or
    /// filtering it, etc.).
    ///
    /// This is not an expression because it breaks the rules that the input
    /// (`e`) must be in the resulting operation. However, like an expression
    /// the result of `transform` is available within the `op` operation.
    Transform,
    /// A step produced as the result of an error.
    ///
    /// Compilation should not report additional errors relating to the node.
    Error,
    /// Special step for Windows.
    ///
    /// These correspond to behaviors that affect how aggregations are computed.
    Window(WindowBehavior),
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "none"),
            Self::Scan { table_id, slice } => match slice {
                None => write!(f, "scan:{table_id}"),
                Some(slice) => write!(f, "scan:{table_id}:{slice:?}"),
            },
            Self::MergeJoin => write!(f, "merge_join"),
            Self::LookupRequest => write!(f, "lookup_request"),
            Self::LookupResponse => write!(f, "lookup_response"),
            Self::WithKey => write!(f, "with_key"),
            Self::Select => write!(f, "select"),
            Self::ShiftTo => write!(f, "shift_to"),
            Self::ShiftUntil => write!(f, "shift_until"),
            Self::Tick(behavior) => write!(f, "tick:{behavior:?}"),
        }
    }
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(literal) => write!(f, "{literal}"),
            Self::LateBound(value) => write!(f, "late_bound:{value:?}"),
            Self::Inst(inst) => write!(f, "{inst}"),
        }
    }
}

/// Implement Display for StepKind to provide the pretty-printed description of
/// the step.
///
/// This representation needs to be parseable to specify the E-graph rewrite
/// rules.
impl fmt::Display for StepKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(operation) => operation.fmt(f),
            Self::Expression(expr) => expr.fmt(f),
            Self::Transform => write!(f, "transform"),
            Self::Window(behavior) => write!(f, "window_behavior:{behavior:?}"),
            Self::Error => write!(f, "error"),
        }
    }
}

impl FromStr for StepKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        if let Some(table_id) = s.strip_prefix("scan:") {
            let table_id = Uuid::parse_str(table_id).context("parsing table id")?;

            Ok(StepKind::Operation(Operation::Scan {
                table_id: TableId::new(table_id),
                slice: None,
            }))
        } else if s == "lookup_request" {
            Ok(StepKind::Operation(Operation::LookupRequest))
        } else if s == "lookup_response" {
            Ok(StepKind::Operation(Operation::LookupResponse))
        } else if s == "select" {
            Ok(StepKind::Operation(Operation::Select))
        } else if s == "shift_to" {
            Ok(StepKind::Operation(Operation::ShiftTo))
        } else if s == "shift_until" {
            Ok(StepKind::Operation(Operation::ShiftUntil))
        } else if s == "with_key" {
            Ok(StepKind::Operation(Operation::WithKey))
        } else if let Ok(literal) = ScalarValue::from_str(s) {
            Ok(StepKind::Expression(Expression::Literal(literal)))
        } else if s == "field_ref" {
            Ok(StepKind::Expression(Expression::Inst(InstKind::FieldRef)))
        } else if s == "merge_join" {
            Ok(StepKind::Operation(Operation::MergeJoin))
        } else if s == "transform" {
            Ok(StepKind::Transform)
        } else if s == "late_bound:ChangedSinceTime" {
            Ok(StepKind::Expression(Expression::LateBound(
                LateBoundValue::ChangedSinceTime,
            )))
        } else {
            inst_from_str(s)
        }
    }
}

fn inst_from_str(inst: &str) -> anyhow::Result<StepKind> {
    InstOp::from_str(inst)
        .with_context(|| format!("parsing instruction '{inst}'"))
        .map(|inst| StepKind::Expression(Expression::Inst(InstKind::Simple(inst))))
}
