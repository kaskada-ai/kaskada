use sparrow_syntax::{Located, Location};

use crate::time_domain::{combine_time_domains, TimeDomain};
use crate::{AstDfgRef, DataContext, DiagnosticCollector};

/// Enumerate the ways that time domains may be checked for a specific function.
///
/// This is generally requiring the arguments to be compatible (possibly after
/// converting continuous arguments to discrete).
///
/// Special cases exist for `ShiftTo` and `ShiftUntil` as described below. These
/// are subject to change. It may be better to use a closure to allow defining
/// the special behaviors as part of each function.
#[derive(Default)]
pub enum TimeDomainCheck {
    /// The function requires the arguments to be compatible, and returns
    /// the resulting time domain.
    ///
    /// This is the default option for functions that don't configure
    /// time domain checking.
    #[default]
    Compatible,
    /// The function is an aggregation.
    ///
    /// This has the same requirements as `Compatible`, but produces
    /// continuous results.
    Aggregation,
    /// The two arguments (value and time) should be checked for compatibility.
    /// This is because they are both computed at the same point, and the time
    /// value is used to determine when the value is shifted to.
    ///
    /// Whatever the result of that is, the resulting TimeDomain is
    /// `shift_to(time)`.
    ShiftTo,
    /// The two arguments (value and predicate) need not be checked for
    /// compatibility. This is because only the value is computed to start
    /// the shifting, and the predicate is then evaluated to determine
    /// whether to emit the shifted values. Thus, they don't need to occur
    /// at the same time,
    ///
    /// Whatever the result of that is, the resulting TimeDomain is
    /// `shift_until(time)`.
    ShiftUntil,
    /// The time domain is a literal.
    #[allow(dead_code)]
    Literal,
}

impl TimeDomainCheck {
    pub fn check_args(
        &self,
        location: &Location,
        diagnostics: &mut DiagnosticCollector<'_>,
        args: &[Located<AstDfgRef>],
        data_context: &DataContext,
    ) -> anyhow::Result<TimeDomain> {
        let combined_input_domain = match self {
            TimeDomainCheck::Compatible
            | TimeDomainCheck::Aggregation
            | TimeDomainCheck::ShiftTo => {
                match combine_time_domains(location, args, data_context)? {
                    Ok(time_domain) => Some(time_domain),
                    Err(diagnostic) => {
                        diagnostic.emit(diagnostics);
                        None
                    }
                }
            }
            TimeDomainCheck::ShiftUntil => None,
            TimeDomainCheck::Literal => {
                anyhow::ensure!(
                    args.is_empty(),
                    "Literal time domain check only supported for no-arg functions"
                );
                None
            }
        };

        let time_domain = match self {
            TimeDomainCheck::Compatible => combined_input_domain.unwrap_or(TimeDomain::Error),
            TimeDomainCheck::Aggregation => {
                if combined_input_domain.is_some() {
                    TimeDomain::Continuous
                } else {
                    TimeDomain::Error
                }
            }
            TimeDomainCheck::ShiftTo => TimeDomain::ShiftTo {
                time: args[0].value(),
            },
            TimeDomainCheck::ShiftUntil => TimeDomain::ShiftUntil {
                predicate: args[0].value(),
            },
            TimeDomainCheck::Literal => TimeDomain::literal(),
        };
        Ok(time_domain)
    }
}
