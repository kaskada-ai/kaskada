use crate::functions::time_domain_check::TimeDomainCheck;
use crate::functions::{Implementation, Registry};

/// The `is_new` pattern used for basic aggregations.
const AGGREGATION_IS_NEW: &str = "(logical_or ?window_is_new ?input_is_new)";

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("count_if<T: any>(input: T, window: window = null) -> u32")
        .with_dfg_signature(
            "count_if<T: any>(input: T, window: bool = null, duration: i64 = null) -> u32",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(count_if ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value",
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("count<T: any>(input: T, window: window = null) -> u32")
        .with_dfg_signature(
            "count_if<T: any>(input: T, window: bool = null, duration: i64 = null) -> u32",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(count_if ({}) ({}) ({}))",
            "transform (is_valid (if ?input_is_new ?input_value)) (merge_join ?input_op \
             ?window_op)",
            "?window_value",
            "?duration_value",
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("sum<N: number>(input: N, window: window = null) -> N ")
        .with_dfg_signature(
            "sum<N: number>(input: N, window: bool = null, duration: i64 = null) -> N",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(sum ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("min<O: ordered>(input: O, window: window = null) -> O")
        .with_dfg_signature(
            "min<O: ordered>(input: O, window: bool = null, duration: i64 = null) -> O",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(min ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("max<O: ordered>(input: O, window: window = null) -> O")
        .with_dfg_signature(
            "max<O: ordered>(input: O, window: bool = null, duration: i64 = null) -> O",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(max ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("mean<N: number>(input: N, window: window = null) -> f64")
        .with_dfg_signature(
            "mean<N: number>(input: N, window: bool = null, duration: i64 = null) -> f64",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(mean ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("variance<N: number>(input: N, window: window = null) -> f64")
        .with_dfg_signature(
            "variance<N: number>(input: N, window: bool = null, duration: i64 = null) -> f64",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(variance ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("stddev<N: number>(input: N, window: window = null) -> f64")
        .with_dfg_signature(
            "stddev<N: number>(input: N, window: bool = null, duration: i64 = null) -> f64",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(powf (variance ({}) ({}) ({})) 0.5f64)",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("last<T: any>(input: T, window: window = null) -> T")
        .with_dfg_signature(
            "last<T: any>(input: T, window: bool = null, duration: i64 = null) -> T",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(last ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);

    registry
        .register("first<T: any>(input: T, window: window = null) -> T")
        .with_dfg_signature(
            "first<T: any>(input: T, window: bool = null, duration: i64 = null) -> T",
        )
        .with_implementation(Implementation::new_pattern(&format!(
            "(first ({}) ({}) ({}))",
            "transform (if ?input_is_new ?input_value) (merge_join ?input_op ?window_op)",
            "?window_value",
            "?duration_value"
        )))
        .with_is_new(Implementation::new_pattern(AGGREGATION_IS_NEW))
        .with_time_domain_check(TimeDomainCheck::Aggregation);
}
