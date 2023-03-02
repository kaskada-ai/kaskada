use std::time::Duration;

use futures::StreamExt;
use itertools::Itertools;

use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record_header::BuildInfo;
use sparrow_qfr::kaskada::sparrow::v1alpha::{FlightRecord, FlightRecordHeader};
use sparrow_qfr::{
    activity, gauge, Activity, FlightRecorderFactory, Gauge, PushRegistration, Registration,
    Registrations,
};

const NUM_OUTPUTS: Gauge<u64> = gauge!("num_outputs");
const MERGING: Activity = activity!("merging");
const GATHER: Activity = activity!("gather", MERGING);
const MERGE: Activity = activity!("merge", MERGING);

static REGISTRATION: Registration = Registration::new(|| {
    let mut r = Registrations::default();
    r.add(NUM_OUTPUTS);
    r.add(MERGING);
    r.add(GATHER);
    r.add(MERGE);
    r
});

inventory::submit!(&REGISTRATION);

/// This example shows how to produce flight records directly.
#[tokio::main]
async fn main() {
    let rx = produce_records().await;
    let records: Vec<_> = tokio_stream::wrappers::ReceiverStream::new(rx)
        .collect()
        .await;

    println!(
        "Header:{:?}",
        FlightRecordHeader::with_registrations(
            "the_request_id".to_owned(),
            BuildInfo {
                sparrow_version: "sparrow_version0.1.0".to_owned(),
                github_ref: "some ref".to_owned(),
                github_sha: "some sha".to_owned(),
                github_workflow: "the workflow".to_owned(),
            }
        )
    );

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
    // We should see:
    // 1. An outer block containing the time spent in MERGING (inclusive of time)
    //    spent in GATHER, and
    // 2. An inner block, containing the time spent exclusively in GATHER.
    let recorder2 = factory.create_recorder("Shift Thread".to_owned()).await;
    {
        let mut _outer = MERGING.start(&recorder2);
        std::thread::sleep(Duration::from_millis(500));
        {
            let mut _inner = GATHER.start(&recorder2);
            std::thread::sleep(Duration::from_millis(750));
        }
    }

    rx
}
