mod error;
mod temp_data_file;
mod testdata;
mod to_record_batch;

pub use error::*;
pub use temp_data_file::*;
pub use testdata::*;
pub use to_record_batch::*;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

static INIT_TEST_LOGGING: std::sync::Once = std::sync::Once::new();

/// Makes sure logging is initialized for test.
///
/// This needs to be called on each test.
pub fn init_test_logging() {
    INIT_TEST_LOGGING.call_once(|| {
        let fmt_layer = tracing_subscriber::fmt::layer().with_test_writer();

        tracing_subscriber::registry()
            .with(EnvFilter::new("egg::=warn,sparrow_=trace,info"))
            .with(fmt_layer)
            .with(tracing_error::ErrorLayer::default())
            .try_init()
            .unwrap();
    });
}
