use anyhow::Context;
use egg::Id;
use itertools::Itertools;
use sparrow_instructions::TableId;
use sparrow_syntax::{Located, Location};

use crate::{AstDfgRef, DataContext, DiagnosticBuilder, DiagnosticCode};

/// The TimeDomain is an approximation of which rows would be defined.
///
/// It is used to report warnings about operations between incompatible
/// domains.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimeDomain {
    /// The TimeDomain represents an error.
    Error,
    /// The TimeDomain represents a continuous value, such as a literal or
    /// aggregation.
    Continuous,
    /// Time domain for values associated wiith a specific table.
    Table { table: TableId },
    /// Time domain for values resulting from a `shift_to`.
    /// The ID should be the expression being shifted to.
    ShiftTo { time: Id },
    /// Time domain for values resulting from a `shift_until`.
    /// The ID should be the expression for the predicate.
    ShiftUntil { predicate: Id },
}

impl TimeDomain {
    pub fn table(table: TableId) -> Self {
        TimeDomain::Table { table }
    }

    pub fn literal() -> Self {
        TimeDomain::Continuous
    }

    pub fn error() -> Self {
        TimeDomain::Error
    }

    pub fn is_continuous(&self) -> bool {
        matches!(self, TimeDomain::Continuous)
    }
}

/// Return the `TimeDomain` of an operation combining the given inputs.
///
/// The outer result indicates whether an unexpected error occurs, where
/// the inner result indicates the user expression produced time domain errors.
#[allow(clippy::match_like_matches_macro)]
pub(super) fn combine_time_domains(
    location: &Location,
    inputs: &[Located<AstDfgRef>],
    data_context: &DataContext,
) -> anyhow::Result<Result<TimeDomain, DiagnosticBuilder>> {
    let unique_time_domains = inputs
        .iter()
        .map(|located| located.with_value(located.time_domain()))
        .filter(|item| match item.inner() {
            TimeDomain::Error => {
                // Ignore errors that have already been reported.
                // If the remaining (non-error) arguments conflict, we'll report
                // an error based on those. Whatever this input really is, the
                // remaining arguments would *still* conflict.
                //
                // Similarly, whatever time domain we choose based on the remaining
                // arguments should be consistent with the result of this. For instance
                // if we had a continuous and non-continuous argument the result would
                // be continuous or non-continuous (depending on which was an error and
                // ignored). But, the continuous result would be OK (could be
                // discretized) and the non-continuous would match the "correct" answer.
                false
            }
            TimeDomain::Continuous => {
                // Due to discretization, continuous results don't matter for determining
                // the resulting time domain.
                false
            }
            _ => true,
        })
        // NOTE: We could have `egg` intern the IDs and use that for comparison. But, that
        // would then rely on simplification to determine that two differently written
        // predicates were identical. Instead, requiring *exactly the same ID* ensures the
        // user wrote the same AST expression for each.
        .unique_by(|arg| (*arg.inner()).clone())
        .at_most_one();

    match unique_time_domains {
        Ok(None) => {
            // All inputs were errors or continuous. The result is continuous
            // which will be compatible with all other time domains.
            Ok(Ok(TimeDomain::Continuous))
        }
        Ok(Some(domain)) => Ok(Ok((*domain.inner()).clone())),
        Err(conflicting) => Ok(Err(diagnose_incompatible_domains(
            location,
            conflicting.collect(),
            data_context,
        )?)),
    }
}

fn diagnose_incompatible_domains(
    location: &Location,
    unique_time_domains: Vec<Located<&'_ TimeDomain>>,
    data_context: &DataContext,
) -> anyhow::Result<DiagnosticBuilder> {
    let mut builder = DiagnosticCode::IncompatibleTimeDomains
        .builder()
        .with_label(
            location
                .primary_label()
                .with_message("Incompatible time domains for operation"),
        );

    // Add information about the distinct time domains observed.
    for time_domain in unique_time_domains {
        // TODO: Improve the error message by printing the time domain in a prettier
        // way. For tables, we should use the table name. For shifts, we should
        // use the shift expression.
        builder = builder.with_label(time_domain.location().secondary_label().with_message({
            match time_domain.inner() {
                TimeDomain::Table { table } => {
                    let table_name = data_context
                        .table_info(*table)
                        .context("Getting table info")?
                        .name();
                    format!("Time Domain: Table '{table_name}'")
                }
                domain => format!("Time Domain: {domain:?}"),
            }
        }));
    }

    Ok(builder)
}
