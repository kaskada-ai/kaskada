use std::io;

use opentelemetry::trace::TraceContextExt;
use serde::ser::{SerializeMap, Serializer as _};
use tracing::{Event, Subscriber};
use tracing_opentelemetry::OtelData;
use tracing_serde::{AsSerde, SerdeMapVisitor};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::{LookupSpan, SpanRef};

/// Custom format for JSON logging events a `request_id`.
pub struct RequestIdFormat;

impl<S, N> FormatEvent<S, N> for RequestIdFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let meta = event.metadata();

        let mut visit = || {
            let mut serializer = serde_json::Serializer::new(WriteAdaptor::new(&mut writer));

            let mut serializer = serializer.serialize_map(None)?;
            serializer.serialize_entry("timestamp", &chrono::Utc::now())?;
            serializer.serialize_entry("level", &meta.level().as_serde())?;
            serializer.serialize_entry("line", &meta.line())?;
            serializer.serialize_entry("module", &meta.module_path())?;
            serializer.serialize_entry("target", meta.target())?;

            if let Some(trace_info) = ctx.lookup_current().as_ref().and_then(lookup_trace_info) {
                serializer
                    .serialize_entry("request_id", &hex::encode(trace_info.trace_id.to_bytes()))?;
                serializer
                    .serialize_entry("span_id", &hex::encode(trace_info.span_id.to_bytes()))?;
            }

            let mut visitor = SerdeMapVisitor::new(serializer);
            event.record(&mut visitor);
            visitor.finish()
        };

        visit().map_err(|_| std::fmt::Error)?;
        writeln!(writer)
    }
}

struct WriteAdaptor<'a> {
    fmt_write: &'a mut dyn std::fmt::Write,
}

impl<'a> WriteAdaptor<'a> {
    fn new(fmt_write: &'a mut dyn std::fmt::Write) -> Self {
        Self { fmt_write }
    }
}

impl<'a> io::Write for WriteAdaptor<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let s =
            std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.fmt_write
            .write_str(s)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(s.as_bytes().len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct TraceInfo {
    pub trace_id: opentelemetry::trace::TraceId,
    pub span_id: opentelemetry::trace::SpanId,
}

fn lookup_trace_info<S>(span_ref: &SpanRef<'_, S>) -> Option<TraceInfo>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    span_ref
        .extensions()
        .get::<OtelData>()
        .map(|otel| TraceInfo {
            trace_id: otel.parent_cx.span().span_context().trace_id(),
            span_id: otel
                .builder
                .span_id
                .unwrap_or(opentelemetry::trace::SpanId::INVALID),
        })
}
