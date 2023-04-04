use std::{env, path::PathBuf};

use prost_wkt_build::{FileDescriptorSet, Message};

#[allow(clippy::print_stdout)]
fn main() {
    let proto_files = &[
        "../../proto/kaskada/kaskada/v1alpha/fenl_diagnostics.proto",
        "../../proto/kaskada/kaskada/v1alpha/schema.proto",
        "../../proto/kaskada/kaskada/v1alpha/sources.proto",
        "../../proto/kaskada/kaskada/v1alpha/destinations.proto",
        "../../proto/kaskada/kaskada/v1alpha/common.proto",
        "../../proto/kaskada/kaskada/v1alpha/file_service.proto",
        "../../proto/kaskada/kaskada/v1alpha/destinations.proto",
        "../../proto/kaskada/kaskada/v1alpha/plan.proto",
        "../../proto/kaskada/kaskada/v1alpha/preparation_service.proto",
        "../../proto/kaskada/kaskada/v1alpha/pulsar.proto",
        "../../proto/kaskada/kaskada/v1alpha/compute_service.proto",
        "../../proto/google/api/field_behavior.proto",
    ];
    println!("cargo:rerun-if-changed=build.rs");
    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={proto_file}");
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("sparrow_descriptor.bin");

    // Configure the feature set for serialization.
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        // All types should be serialize/deserialize
        .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]")
        // Register serde types for well known types.
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .type_attribute("kaskada.v1alpha.PreparedFile", "#[derive(Eq, Hash)]")
        .type_attribute(
            "kaskada.v1alpha.SourceFile.source_path",
            "#[serde(rename_all = \"snake_case\")]",
        )
        .field_attribute(
            "kaskada.v1alpha.SourceFile.source_path",
            "#[serde(flatten)]",
        )
        .field_attribute("kaskada.v1alpha.FeatureSet.formulas", "#[serde(default)]")
        .field_attribute(
            "kaskada.v1alpha.Formula.source_location",
            "#[serde(default)]",
        )
        // Add some annotations to allow the following to work with clap.
        .type_attribute(
            "kaskada.v1alpha.ExecuteRequest.Limits",
            "#[derive(clap::Args)] #[command(rename_all= \"kebab-case\")]",
        )
        .field_attribute(
            "kaskada.v1alpha.ExecuteRequest.Limits.preview_rows",
            "#[arg(long, default_value_t = 0)]",
        )
        .type_attribute(
            "kaskada.v1alpha.LateBoundValue",
            "#[derive(clap::Subcommand, enum_map::Enum)]",
        )
        .compile(proto_files, &["../../proto"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {e}"));

    let descriptor_bytes = std::fs::read(descriptor_path).unwrap();
    let descriptor = FileDescriptorSet::decode(&descriptor_bytes[..]).unwrap();

    prost_wkt_build::add_serde(out_dir, descriptor);
}
