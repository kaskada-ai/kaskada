use std::time::Duration;

use futures::StreamExt;
use itertools::Itertools;

use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecord;
use sparrow_qfr::{activity, gauge, Activity, FlightRecorderFactory, Gauge};

const NUM_OUTPUTS: Gauge<u64> = gauge!("num_outputs");
inventory::submit!(NUM_OUTPUTS.registration());

const MERGING: Activity = activity!("merging");
inventory::submit!(MERGING.registration());

const GATHER: Activity = activity!("gather", MERGING);
inventory::submit!(GATHER.registration());

const MERGE: Activity = activity!("merge", MERGING);
inventory::submit!(MERGE.registration());

/// This example shows how to produce flight records directly.
#[tokio::main]
async fn main() {
    let rx = produce_records().await;
    let records: Vec<_> = tokio_stream::wrappers::ReceiverStream::new(rx)
        .collect()
        .await;

    println!(
        "Records:\n{}",
        records
            .iter()
            .format_with("\n", |elt, f| f(&format_args!("  {elt:?}")))
    );
}

async fn produce_records() -> tokio::sync::mpsc::Receiver<FlightRecord> {
    let (tx, rx) = tokio::sync::mpsc::channel(1000);

    let mut factory = FlightRecorderFactory::new(tx).await;
    let recorder1 = factory.create_recorder("Merge Thread".to_owned()).await;

    // Time some synchronous code.
    let mut activation = MERGING.start(&recorder1);
    std::thread::sleep(Duration::from_secs(1));

    // Explicitly finish. Could also let the activation drop.
    activation.report_metric(NUM_OUTPUTS, 57);
    activation.finish();

    // Time some synchronous code with nesting.
    let recorder2 = factory.create_recorder("Shift Thread".to_owned()).await;
    {
        let mut _outer = MERGING.start(&recorder2);
        std::thread::sleep(Duration::from_millis(500));
        {
            let mut _inner = MERGING.start(&recorder2);
            std::thread::sleep(Duration::from_millis(750));
        }
    }

    rx
}
