use sparrow_instructions::InstOp;

use crate::functions::time_domain_check::TimeDomainCheck;
use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("time_of<T: any>(input: T) -> timestamp_ns")
        .with_implementation(Implementation::Instruction(InstOp::TimeOf));

    registry
        .register("when<T: any>(condition: bool, value: T) -> T")
        .with_implementation(Implementation::new_pattern(
            "(transform (transform ?value_value (merge_join ?condition_op ?value_op)) (select \
             (transform ?condition_value (merge_join ?condition_op ?value_op))))",
        ))
        .with_is_new(Implementation::new_pattern(
            // We need to filter the `is_new` exactly the same way we filtered the
            //  value, otherwise entities that exist in the `is_new` may cause
            //  later passes to tick in cases that should have been filtered
            //  out.

            //  The value to filter -- `(logical_or ?value_is_new ?condition_is_new)`
            //  is the default definition of `is_new`, and seems reasonable here --
            //  the result is new if any of the inputs are new.
            {
                const MERGED_OP: &str = "(merge_join ?value_op ?condition_op)";
                const VALUE_IS_NEW: &str =
                    const_format::formatcp!("(transform ?value_is_new {MERGED_OP})");
                const CONDITION_IS_NEW: &str =
                    const_format::formatcp!("(transform ?condition_is_new {MERGED_OP})");
                const VALUE_OR_CONDITION: &str = const_format::formatcp!(
                    "(transform (logical_or {VALUE_IS_NEW} {CONDITION_IS_NEW}) {MERGED_OP})"
                );
                const CONDITION_VALUE: &str =
                    const_format::formatcp!("(transform ?condition_value {MERGED_OP})");

                const_format::formatcp!(
                    "(transform {VALUE_OR_CONDITION} (select {CONDITION_VALUE}))"
                )
            },
        ));

    registry
        .register("shift_to<T: any>(time: timestamp_ns, value: T) -> T")
        .with_implementation(Implementation::new_pattern(
            "(transform ?value_value (shift_to ?time_value))",
        ))
        .with_is_new(Implementation::new_pattern(
            "(transform ?value_is_new (shift_to ?time_value))",
        ))
        .with_time_domain_check(TimeDomainCheck::ShiftTo);

    // Shift by is a macro expansion of shift to, hence it shared the same time domain check
    registry
        .register("shift_by<T: any, D: timedelta>(delta: D, value: T) -> T")
        .with_implementation(Implementation::new_fenl_rewrite(
            "shift_to(add_time(delta, time_of(value)), value)",
        ))
        .with_is_new(Implementation::new_pattern(
            "(transform ?value_is_new (shift_to (add_time ?delta_value (time_of ?value_value))))",
        ))
        .with_time_domain_check(TimeDomainCheck::ShiftTo);

    registry
        .register("days(days: i64) -> interval_days")
        .with_implementation(Implementation::Instruction(InstOp::Days));

    registry
        .register("day_of_month(time: timestamp_ns) -> u32")
        .with_implementation(Implementation::Instruction(InstOp::DayOfMonth));

    registry
        .register("day_of_month0(time: timestamp_ns) -> u32")
        .with_implementation(Implementation::Instruction(InstOp::DayOfMonth0));

    registry
        .register("day_of_year(time: timestamp_ns) -> u32")
        .with_implementation(Implementation::Instruction(InstOp::DayOfYear));

    registry
        .register("day_of_year0(time: timestamp_ns) -> u32")
        .with_implementation(Implementation::Instruction(InstOp::DayOfYear0));

    registry
        .register("months(months: i64) -> interval_months")
        .with_implementation(Implementation::Instruction(InstOp::Months));

    registry
        .register("month_of_year(time: timestamp_ns) -> u32")
        .with_implementation(Implementation::Instruction(InstOp::MonthOfYear));

    registry
        .register("month_of_year0(time: timestamp_ns) -> u32")
        .with_implementation(Implementation::Instruction(InstOp::MonthOfYear0));

    registry
        .register("year(time: timestamp_ns) -> i32")
        .with_implementation(Implementation::Instruction(InstOp::Year));

    registry
        .register("seconds(seconds: i64) -> duration_s")
        .with_implementation(Implementation::Instruction(InstOp::Seconds));

    registry
        .register("add_time<D: timedelta>(delta: D, time: timestamp_ns) -> timestamp_ns")
        .with_implementation(Implementation::Instruction(InstOp::AddTime));

    registry
        .register("shift_until<T: any>(predicate: bool, value: T) -> T")
        .with_implementation(Implementation::new_pattern(
            "(transform (transform ?value_value (merge_join ?value_op ?predicate_op)) \
             (shift_until (transform ?predicate_value (merge_join ?value_op ?predicate_op))))",
        ))
        .with_is_new(Implementation::new_pattern(
            "(transform (transform ?value_is_new (merge_join ?value_op ?predicate_op)) \
             (shift_until (transform ?predicate_value (merge_join ?value_op ?predicate_op))))",
        ))
        .with_time_domain_check(TimeDomainCheck::ShiftUntil);

    registry
        .register("seconds_between(t1: timestamp_ns, t2: timestamp_ns) -> duration_s")
        .with_implementation(Implementation::Instruction(InstOp::SecondsBetween));

    registry
        .register("days_between(t1: timestamp_ns, t2: timestamp_ns) -> interval_days")
        .with_implementation(Implementation::Instruction(InstOp::DaysBetween));

    registry
        .register("months_between(t1: timestamp_ns, t2: timestamp_ns) -> interval_months")
        .with_implementation(Implementation::Instruction(InstOp::MonthsBetween));

    // Note: Lag is specifically *not* an aggregation function.
    registry
        .register("lag<O: ordered>(const n: i64, input: O) -> O")
        .with_implementation(Implementation::new_fenl_rewrite(
            "input | collect(min=n+1, max=n+1) | index(0)",
        ));
}
