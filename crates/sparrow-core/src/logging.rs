
use std::{env, path::PathBuf};

use prost::Message;
use prost_reflect::{MessageDescriptor, ReflectMessage};
use prost_types::FileDescriptorSet;
use prost_reflect::{DynamicMessage, DescriptorPool, Value};
use static_init::dynamic;

#[dynamic]
static POOL: FileDescriptorSet = {
    let proto_files = [
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
    prost_reflect_build::Builder::new()
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&proto_files, &["../../proto"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {e}"));

    let descriptor_bytes = std::fs::read(descriptor_path).unwrap();
    let descriptor = FileDescriptorSet::decode(&descriptor_bytes[..]).unwrap();
    descriptor
};


// The string to use when redacting a sensitive field
const REDACTED_STRING: &str = "...redacted...";

/// Redacts the contents of a protobuf message if the field is marked as sensitive.
/// 
/// This function should be used when logging any protobuf message to ensure sensitive
/// information is not logged.
pub fn redact_sensitive<M: prost::Message + ReflectMessage>(msg: M) -> M {
    // let pool = DescriptorPool::decode(include_bytes!("file_descriptor_set.bin").as_ref()).unwrap();
    // let message_descriptor = pool.get_message_by_name("package.MyMessage").unwrap();
    // let dynamic_message = DynamicMessage::decode(message_descriptor, b"\x08\x96\x01".as_ref()).unwrap();
    let message_descriptor = msg.descriptor();
    message_descriptor.fields().for_each(|field_descriptor| {
        if field_descriptor.field_descriptor_proto().options.unwrap().get_extension(kaskada::kaskada::v1alpha::sensitive).unwrap() {
            redact_field(&mut msg, field_descriptor.name());
        }
    });
    // let root_message = M::default();
    // let mut field_value = msg.get_reflect(field.number());
    // let sub_message_descriptor = message_descriptor.get_field_descriptor(field_descriptor.number()).unwrap().message_descriptor().unwrap();
    // let sub_message = value.get_reflection().get_message(value, field_descriptor).unwrap();

    todo!()

    
    // let descriptor = msg.descriptor();
    // let field = descriptor.fields().iter()
    //     .find(|(f, _)| f.name() == field_name)
    //     .expect("Field not found");

    // let mut field_value = msg.get_reflect(field.number());
    // if field.kind() == reflect::FieldDescriptorProto_Type::TYPE_STRING {
    //     field_value.set(ProtobufValue::String("REDACTED".to_string()));
    // } else {
    //     field_value.clear();
    // }
}

// fn test(msg: &dyn Message) {
//     let pool = DescriptorPool::decode(include_bytes!("file_descriptor_set.bin").as_ref()).unwrap();
// let message_descriptor = pool.get_message_by_name("package.MyMessage").unwrap();

// let dynamic_message = DynamicMessage::decode(message_descriptor, b"\x08\x96\x01".as_ref()).unwrap();

// assert_eq!(dynamic_message.get_field_by_name("foo").unwrap().as_ref(), &Value::I32(150));
// }