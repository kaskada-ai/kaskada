use hashbrown::HashMap;

use super::agg_fn::AggFn;

/// Placeholder struct for the implementation of the [[AggFn]] for `First` on
/// strings.
pub struct FirstString {}
impl AggFn for FirstString {
    type InT = String;
    type AccT = Option<String>;
    type OutT = String;

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
/// strings.
pub struct LastString {}
impl AggFn for LastString {
    type InT = String;
    type AccT = Option<String>;
    type OutT = String;

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

/// Placeholder struct for the implementation of the [[AggFn]] for `Top` on
/// strings.
pub struct TopString {}
impl AggFn for TopString {
    type InT = String;
    type AccT = HashMap<String, i64>;
    type OutT = String;

    fn zero() -> Self::AccT {
        HashMap::new()
    }

    fn merge(acc1: &mut Self::AccT, acc2: &Self::AccT) {
        for (key, value) in acc2.iter() {
            if let Some(entry) = acc1.get_mut(key) {
                *entry += value;
            } else {
                acc1.insert(key.clone(), *value);
            }
        }
    }

    fn extract(acc: &Self::AccT) -> Option<Self::OutT> {
        // This is extremely inefficient. A better solution may be to use a
        // priority queue/max heap here.
        let max_item = acc.iter().max_by(|a, b| {
            if a.1 != b.1 {
                a.1.cmp(b.1)
            } else {
                b.0.cmp(a.0)
            }
        });
        if let Some(max) = max_item {
            Some(max.0.clone())
        } else {
            None
        }
    }

    fn add_one(acc: &mut Self::AccT, input: &Self::InT) {
        if let Some(entry) = acc.get_mut(input) {
            *entry += 1;
        } else {
            acc.insert(input.clone(), 1);
        }
    }

    fn name() -> &'static str {
        "top_string"
    }
}
