use sparrow_api::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;
use sparrow_syntax::WindowBehavior;

use crate::functions::time_domain_check::TimeDomainCheck;
use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("since(condition: bool) -> window")
        .with_implementation(Implementation::Window(WindowBehavior::Since))
        .with_is_new(Implementation::new_pattern("?condition_value"));

    registry
        .register("sliding(const duration: i64, condition: bool) -> window")
        .with_implementation(Implementation::Window(WindowBehavior::Sliding))
        .with_is_new(Implementation::new_pattern("?condition_value"));

    registry
        .register("minutely() -> bool")
        .with_implementation(Implementation::Tick(TickBehavior::Minutely))
        .with_is_new(Implementation::Tick(TickBehavior::Minutely))
        .with_time_domain_check(TimeDomainCheck::Compatible);

    registry
        .register("hourly() -> bool")
        .with_implementation(Implementation::Tick(TickBehavior::Hourly))
        .with_is_new(Implementation::Tick(TickBehavior::Hourly))
        .with_time_domain_check(TimeDomainCheck::Compatible);

    registry
        .register("daily() -> bool")
        .with_implementation(Implementation::Tick(TickBehavior::Daily))
        .with_is_new(Implementation::Tick(TickBehavior::Daily))
        .with_time_domain_check(TimeDomainCheck::Compatible);

    registry
        .register("monthly() -> bool")
        .with_implementation(Implementation::Tick(TickBehavior::Monthly))
        .with_is_new(Implementation::Tick(TickBehavior::Monthly))
        .with_time_domain_check(TimeDomainCheck::Compatible);

    registry
        .register("yearly() -> bool")
        .with_implementation(Implementation::Tick(TickBehavior::Yearly))
        .with_is_new(Implementation::Tick(TickBehavior::Yearly))
        .with_time_domain_check(TimeDomainCheck::Compatible);

    registry
        .register("finished() -> bool")
        .with_implementation(Implementation::Tick(TickBehavior::Finished))
        .with_is_new(Implementation::Tick(TickBehavior::Finished))
        .with_time_domain_check(TimeDomainCheck::Compatible)
        .set_internal();
}
