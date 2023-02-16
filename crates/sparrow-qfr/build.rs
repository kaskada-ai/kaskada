#[allow(clippy::print_stdout)]
fn main() {
    let proto_files = &["proto/kaskada/sparrow/v1alpha/query_flight_record.proto"];
    println!("cargo:rerun-if-changed=build.rs");
    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={proto_file}");
    }

    // Build the proto files
    tonic_build::configure()
        .type_attribute("kaskada.sparrow.v1alpha.ThreadId", "#[derive(Eq, Hash)]")
        .compile(proto_files, &["proto", "../../proto"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {e}"));
}
