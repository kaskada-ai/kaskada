load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_scala_annex//rules:scala_proto.bzl", "scala_proto_library")
load("//bazel/rules_scala:scala.bzl", "generated_scala_library")

proto_library(
    name = "proto",
    srcs = [
        "compute.proto",
    ],
    visibility = ["//visibility:public"],
    deps = ["@go_googleapis//google/api:annotations_proto"],
)

scala_proto_library(
    name = "scala_proto",
    deps = [":proto"],
)

generated_scala_library(
    name = "generated_scala_library",
    srcs = [
        ":scala_proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:com_thesamet_scalapb_lenses_2_13",
        "@maven//:com_thesamet_scalapb_scalapb_runtime_2_13",
        "@maven//:com_thesamet_scalapb_scalapb_runtime_grpc_2_13",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_stub",
    ],
)
