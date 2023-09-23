use schema_registry_converter::{
    async_impl::schema_registry::{get_schema_by_subject, SrSettings},
    schema_registry_common::SubjectNameStrategy,
};

use error_stack::{IntoReport, Result, ResultExt};

/// TODO: Pending Implementation
#[allow(unused)]
#[derive(Debug, derive_more::Display)]
pub enum Error {
    #[allow(dead_code)]
    AvroNotEnabled,
    AvroSchemaConversion,
    SchemaRequest,
    UnsupportedSchema,
}
impl error_stack::Context for Error {}

#[allow(unused)]
/// TODO: Pending implementation
pub async fn get_kafka_schema() -> Result<String, Error> {
    let sr_settings = SrSettings::new(String::from("http://localhost:8085"));
    let subject_name_strategy =
        SubjectNameStrategy::TopicNameStrategy(String::from("my-topic"), false);
    let schema = get_schema_by_subject(&sr_settings, &subject_name_strategy)
        .await
        .into_report()
        .change_context(Error::SchemaRequest)?;
    Ok(schema.schema)
}
