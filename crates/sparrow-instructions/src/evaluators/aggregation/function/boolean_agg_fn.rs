use super::agg_fn::AggFn;

/// Placeholder struct for the implementation of the [[AggFn]] for `First` on
/// booleans.
pub struct FirstBoolean {}
impl AggFn for FirstBoolean {
    type InT = bool;
    type AccT = Option<bool>;
    type OutT = bool;

    fn zero() -> Self::AccT {
        None
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if acc1.is_none() {
            *acc1 = acc2.to_owned()
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        acc.as_ref().map(|s| s.to_owned())
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        if acc.is_none() {
            *acc = Some(input.to_owned())
        }
    }

    fn name() -> &'static str {
        "first_string"
    }
}

/// Placeholder struct for the implementation of the [[AggFn]] for `Last` on
/// booleans.
pub struct LastBoolean {}
impl AggFn for LastBoolean {
    type InT = bool;
    type AccT = Option<bool>;
    type OutT = bool;

    fn zero() -> Self::AccT {
        None
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        if acc2.is_some() {
            *acc1 = acc2.to_owned()
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        acc.as_ref().map(|s| s.to_owned())
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        *acc = Some(input.to_owned())
    }

    fn name() -> &'static str {
        "last_string"
    }
}
