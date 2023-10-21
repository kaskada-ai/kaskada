use sparrow_batch::{Batch, RowTime};

/// The result of a gather operation
#[derive(Debug, PartialEq)]
pub struct GatheredBatches {
    /// For each participant in the gather, a sequence of gathered input batches.
    ///
    /// This may be multiple batches for each input stream -- for instance, the
    /// slice of a previous batch that wasn't emitted or a few batches that were
    /// collected while waiting for a different input stream to receive a batch
    /// and "unblock" gathering up to a certain tiime.
    ///
    /// We don't concatenate the input batches because that would lead to
    /// allocating and copying data into the concatenated chunk. Instead, we
    /// leave the batches separate (here) and concatenate later.
    pub batches: Vec<Vec<Batch>>,

    /// The time up to which the batches are "complete".
    ///
    /// Any rows in future gathered batches must be strictly greater than this.
    pub up_to_time: RowTime,
}
