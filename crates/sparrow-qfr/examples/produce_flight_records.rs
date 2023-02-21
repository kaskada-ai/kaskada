use futures::StreamExt;
use itertools::Itertools;
use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::{
    execute_pass_metrics, EventType, ExecutePassMetrics, Metrics,
};
use sparrow_qfr::kaskada::sparrow::v1alpha::thread_id::ThreadKind;
use sparrow_qfr::kaskada::sparrow::v1alpha::{FlightRecord, ThreadId};
use sparrow_qfr::FlightRecorderFactory;

/// This example shows how to produce flight records directly.
#[tokio::main]
async fn main() {
    let rx = produce_records();
    let records: Vec<_> = tokio_stream::wrappers::ReceiverStream::new(rx)
        .concat()
        .await;

    println!(
        "Records:\n{}",
        records
            .iter()
            .format_with("\n", |elt, f| f(&format_args!("  {elt:?}")))
    );
}

fn produce_records() -> tokio::sync::mpsc::Receiver<Vec<FlightRecord>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1000);
    let factory = FlightRecorderFactory::new(tx);

    // The old flight records had a hard-coded way of identifying threads.
    // This should be more extensible. When you create a thread you provide
    // the metadata, and that gives you back an "ID" to associate those
    // records with.
    let mut operation1 = factory.create_for_thread(ThreadId {
        pass_id: 0,
        kind: ThreadKind::Merge as i32,
        thread_id: 0,
    });

    // The old flight records had a hard-coded set of "event types" that could
    // be recorded. They could be recorded at the root ("index=0") or against
    // specific instructions.
    operation1.record_event(EventType::ExecutePass {}, 0, None);

    let mut operation2 = factory.create_for_thread(ThreadId {
        pass_id: 0,
        kind: ThreadKind::Execute as i32,
        thread_id: 1,
    });

    // The old flight records had "hard-coded" rules for things like
    // "Instructions may be executed within a pass". We should move
    // away from this -- instead, a pass should be able to report
    // metrics for each instruction.
    operation2.record_event(
        EventType::ExecutePassInstruction {},
        1,
        Some(Metrics::ExecutePass(ExecutePassMetrics {
            num_rows: 100,
            num_cumulative_entities: 1000,
            cause: execute_pass_metrics::Cause::Inputs as i32,
        })),
    );

    // Adding timing information used a separate path. This is necessary
    // because futures need to be wrapped in special async-timing code.
    //
    // The usage was "put the input to the timed", then use `and_then` and
    // `and_then_async` to chain things, and then call `record` at the end.
    let _ = sparrow_qfr::Timed::new(1).and_then(|n| n + 1).record(
        &mut operation2,
        EventType::ExecutePassInstruction {},
        1,
    );

    rx
}
