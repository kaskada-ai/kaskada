use std::time::Duration;

use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::resource::ResourceDetector;
use opentelemetry::sdk::trace::Tracer;
use opentelemetry::sdk::Resource;
use opentelemetry_otlp::WithExportConfig;
use request_id_format::RequestIdFormat;
use tonic::codegen::http;
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::BuildInfo;

mod request_id_format;

/// The options available for configuring tracing.
///
/// The Jaeger exporter should also support the environment variables documented
/// here: <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/sdk-environment-variables.md#jaeger-exporter>
///
/// The options `OTEL_EXPORTER_JAEGER_ENDPOINT`, `OTEL_EXPORTER_JAEGER_USER` and
/// `OTEL_EXPORTER_JAEGER_PASSWORD` are only supported if the `collector_client`
/// feature of the `opentelemetry-jaeger` crate are enabled.
// TODO: Consider replicating the supported options so they show up in --help
#[derive(clap::Args, Debug)]
pub struct TracingOptions {
    /// Log & tracing filter configuration.
    ///
    /// Defaults to `egg::=warn,sparrow_=trace,info` which omits most traces
    /// from the `egg` crate (because it's very log-heavy), and `trace` from
    /// any of the sparrow crates (`sparrow_runtime`, `sparrow_main`, etc.),
    /// and `info` from other crates.
    #[arg(
        long,
        default_value = "egg::=warn,sparrow_=trace,info",
        env = "SPARROW_LOG_FILTER"
    )]
    log_filters: String,
    /// Whether OpenTelemetry tracing should be enabled.
    #[arg(long, env = "SPARROW_OTEL_TRACING_ENABLED")]
    tracing_enabled: bool,
    /// Whether logs should be exported as JSON.
    ///
    /// When JSON logs are enabled we include the Trace ID in log
    /// messages and omit parent spans information.
    #[arg(long, env = "SPARROW_LOG_JSON")]
    log_json: bool,

    /// Whether to disable color output in logs.
    /// 
    /// Set to `1` to disable color output.
    #[arg(long, default_value = "0", env = "NO_COLOR")]
    log_no_color: i8,
}

impl TracingOptions {
    fn create_tracer<T>(&self) -> Option<OpenTelemetryLayer<T, Tracer>>
    where
        T: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        if self.tracing_enabled {
            // Setup tracing reporting to open telemetry via OTLP.
            let resource = Resource::from_detectors(
                Duration::new(5, 0),
                vec![
                    Box::new(opentelemetry::sdk::resource::EnvResourceDetector::new()),
                    Box::new(opentelemetry::sdk::resource::OsResourceDetector),
                    Box::new(opentelemetry::sdk::resource::ProcessResourceDetector),
                    Box::new(opentelemetry::sdk::resource::SdkProvidedResourceDetector),
                    Box::new(SparrowBuildResourceDetector),
                ],
            );
            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_trace_config(
                    opentelemetry::sdk::trace::Config::default().with_resource(resource),
                )
                .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
                .install_batch(opentelemetry::runtime::Tokio)
                .unwrap();

            // Configure propagation. This is only necessary for the gRPC serving, but
            // doesn't hurt other commands with nothing to propagate.
            global::set_text_map_propagator(TraceContextPropagator::new());

            Some(tracing_opentelemetry::layer().with_tracer(tracer))
        } else {
            None
        }
    }
}

/// Setup tracing to both Jaeger and local console.
///
/// # Panics
/// If any of the setup fails. This is called early on and is required to work
/// in order for logging / debugging / etc.
pub fn setup_tracing(config: &TracingOptions) {
    // Enable filtering based on `log_filters`.
    // Do this early so that we can exclude logs (and traces) from spammy components
    // such as egg.
    let registry = tracing_subscriber::registry()
        .with(EnvFilter::try_new(&config.log_filters).unwrap())
        .with(tracing_error::ErrorLayer::default());

    // Add logging and install the handler. We do this in one block
    // since the type of logs (JSON or not) affects the type of the
    // format layer and registry.
    if config.log_json {
        let fmt_layer = tracing_subscriber::fmt::layer().with_ansi(config.log_no_color != 1).event_format(RequestIdFormat);
        registry
            .with(fmt_layer)
            .with(config.create_tracer())
            .try_init()
            .unwrap();
    } else {
        let fmt_layer = tracing_subscriber::fmt::layer().with_ansi(config.log_no_color != 1).with_target(false);
        registry
            .with(fmt_layer)
            .with(config.create_tracer())
            .try_init()
            .unwrap();
    }
}

struct SparrowBuildResourceDetector;

impl ResourceDetector for SparrowBuildResourceDetector {
    fn detect(&self, _timeout: std::time::Duration) -> Resource {
        BuildInfo::default().as_resource()
    }
}

/// Create an initial span with the opentelemetry context
///
/// The intended use is as the `trace_fn` in the gRPC server. This ensures that
/// the request is executed in a properly connected span.
pub fn propagate_span(request: &http::Request<()>) -> tracing::Span {
    let header_map = HeaderMap(request.headers());
    let parent_cx = global::get_text_map_propagator(|prop| prop.extract(&header_map));
    let span = tracing::info_span!("Start of request");

    // Set the OTEL parent of the tracing span. This ties things into the
    // overall OTEL trace.
    span.set_parent(parent_cx);
    span
}

/// Wrapper forf extending HeaderMap.
struct HeaderMap<'a>(&'a http::header::HeaderMap);

impl<'a> Extractor for HeaderMap<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        // Get a value for a key from the headers.
        // If the value can't be converted to &str, returns None
        self.0.get(key).and_then(|h| h.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        // Collect all the keys from the headers.
        self.0.keys().map(|k| k.as_str()).collect::<Vec<_>>()
    }
}
