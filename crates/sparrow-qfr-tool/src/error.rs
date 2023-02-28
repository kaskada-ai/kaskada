use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::Record;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "internal error")]
    Internal,
    #[display(fmt = "unsupported flight record: {_0:?}")]
    UnsupportedRecord(Option<Record>),
    #[display(fmt = "undefined activity ID")]
    UndefinedActivityId,
    #[display(fmt = "undefined metric ID")]
    UndefinedMetricId,
    #[display(fmt = "missing metric value")]
    MissingMetricValue,
}

impl error_stack::Context for Error {}
