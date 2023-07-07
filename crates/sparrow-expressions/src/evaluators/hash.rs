use std::sync::Arc;

use arrow_array::{ArrayRef, UInt64Array};
use arrow_buffer::Buffer;
use error_stack::{IntoReport, ResultExt};
use sparrow_arrow::hasher::Hasher;

use crate::evaluator::Evaluator;
use crate::evaluators::StaticInfo;
use crate::values::ArrayRefValue;
use crate::work_area::WorkArea;
use crate::Error;

use std::cell::RefCell;

inventory::submit!(crate::evaluators::EvaluatorFactory {
    name: "hash",
    create: &create
});

/// Evaluator for `hash`.
struct HashEvaluator {
    input: ArrayRefValue,
}

thread_local! {
    /// Thread-local hasher.
    ///
    /// TODO: Move this to the hasher and make it easy to automatically
    /// use this instance.
    static HASHER: RefCell<Hasher> = RefCell::new(Hasher::default());
}

impl Evaluator for HashEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let hashes = HASHER.with(|hasher| -> error_stack::Result<UInt64Array, Error> {
            let mut hasher = hasher.borrow_mut();
            let hashes = hasher
                .hash_array(input.as_ref())
                .change_context(Error::ExprEvaluation)?;
            let hashes = Buffer::from_slice_ref(hashes);
            UInt64Array::try_new(hashes.into(), None)
                .into_report()
                .change_context(Error::ExprEvaluation)
        })?;

        Ok(Arc::new(hashes))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(HashEvaluator {
        input: input.array_ref(),
    }))
}
