use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::aggregation::function::AggFn;

/// Represents a partial window on a stack in the two-stacks implemention of an
/// aggregation.
///
/// Since this algorithm requires two accumulators, we have to perform 2*n
/// updates. There are optimizations we can make by deferring the cumulative
/// update until it is needed.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WindowPart<AccT> {
    accum: AccT,
    cumulative: AccT,
}

impl<AccT> WindowPart<AccT> {
    pub fn new(accum: AccT, cumulative: AccT) -> Self {
        Self { accum, cumulative }
    }
}

/// Accumulator for windowed aggregation using Two Stacks implementation.
///
/// The bound indicates that the serde on this struct requires only
/// `AggF::AccT` to implement `Serialize` and `DeserializeOwned`.
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "AggF: AggFn, AggF::AccT: Serialize",
    deserialize = "AggF: AggFn, AggF::AccT: DeserializeOwned"
))]
pub struct TwoStacks<AggF: AggFn> {
    /// Window segments that haven't been "flipped". Newest at the end.
    incoming: Vec<WindowPart<AggF::AccT>>,
    /// Window segments that have been "flipped". Oldest at the end.
    outgoing: Vec<WindowPart<AggF::AccT>>,
}

impl<AggF: AggFn> std::fmt::Debug for TwoStacks<AggF> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoStacks")
            .field("incoming", &self.incoming)
            .field("outgoing", &self.outgoing)
            .finish()
    }
}

impl<AggF> Clone for TwoStacks<AggF>
where
    AggF: AggFn,
    AggF::AccT: Clone,
{
    fn clone(&self) -> Self {
        Self {
            incoming: self.incoming.clone(),
            outgoing: self.outgoing.clone(),
        }
    }
}

impl<AggF: AggFn> TwoStacks<AggF> {
    pub fn new(num_windows: i64) -> Self {
        // Initialize the `incoming` stack with `num_windows` elements. This ensures the
        // first time we see an input for a key, we have the correct number of window
        // parts.
        Self {
            incoming: vec![WindowPart::new(AggF::zero(), AggF::zero()); num_windows as usize],
            outgoing: Vec::new(),
        }
    }

    /// Returns the current aggregate value.
    ///
    /// This method will return the partially aggregated value of the oldest
    /// existing window if a window is not closing at this time.
    pub fn accum_value(&self) -> AggF::AccT {
        // The accumulator must be cloned to avoid mutating the existing one.
        // Note the `outgoing` stack contains values occurring earlier than the
        // `incoming` stack, so we merge the `incoming` into the `outgoing`.
        match self.outgoing() {
            Some(outgoing) => {
                let mut accum = outgoing.cumulative.clone();
                AggF::merge(&mut accum, &self.incoming().cumulative);
                accum
            }
            None => self.incoming().cumulative.clone(),
        }
    }

    /// Adds a single input to the next `incoming` window part.
    pub fn add_input(&mut self, input: &AggF::InT) {
        AggF::add_one(&mut self.incoming_mut().accum, input);

        // TODO: If we have a lot of `add_input` calls, we could make them faster by
        // deferring the update to cumulative. Basically, the top "cumulative"
        // isn't set until the next item is pushed. This would affect
        // `accum_value`, etc. One easy way to do this would be to say
        // that every element *doesn't* include it's accumulator?
        AggF::add_one(&mut self.incoming_mut().cumulative, input);
    }

    /// Evicts the oldest window
    pub fn evict(&mut self) {
        if self.outgoing.is_empty() {
            self.flip();
        };
        self.outgoing.pop();
        let recent_cumulative = match self.incoming.last() {
            Some(part) => part.cumulative.clone(),
            None => AggF::zero(),
        };
        self.incoming
            .push(WindowPart::new(AggF::zero(), recent_cumulative));
    }

    fn flip(&mut self) {
        debug_assert!(self.outgoing.is_empty());
        std::mem::swap(&mut self.incoming, &mut self.outgoing);
        self.outgoing.reverse();

        // Fix up the cumulatives to reflect the new reversed order.
        // Each item should be the sum of its accumulator and the cumulative
        // values below it.
        let mut accum = AggF::zero();
        for outgoing in &mut self.outgoing {
            AggF::merge(&mut accum, &outgoing.accum);
            outgoing.cumulative = accum.clone();
        }
    }

    fn incoming(&self) -> &WindowPart<AggF::AccT> {
        self.incoming.last().unwrap_or_else(|| {
            panic!(
                "We always expect the incoming stack to have 1 or more window parts, since we \
                 will always create a new window part when one is evicted."
            )
        })
    }

    fn incoming_mut(&mut self) -> &mut WindowPart<AggF::AccT> {
        self.incoming.last_mut().unwrap_or_else(|| {
            panic!(
                "We always expect the incoming stack to have 1 or more window parts, since we \
                 will always create a new window part when one is evicted."
            )
        })
    }

    fn outgoing(&self) -> Option<&WindowPart<AggF::AccT>> {
        self.outgoing.last()
    }
}
