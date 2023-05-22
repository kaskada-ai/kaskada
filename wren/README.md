# Wren

Wren is the API layer between external client libraries, and Compute.  It also manages table schema & metadata.

## Building

### Notes

* To manage dependences, wren uses [go-modules](https://golang.org/ref/mod).
  * The `go.mod` file is defined in the root wren folder.
* To generate gRPC/protobuf libraries, wren uses [Buf](https://buf.build)
  * The `buf.gen.yaml` files are defined in the root proto folder


### Building Wren

Note that building wren currently requires docker.  Docker is used for generating code for `gRPC` via `buf`.

* `make proto/generate`
  * writes the output to `gen/` -- all the files execpt those that match the pattern `*.ent.go` are dyamically generated. do not commit them to the repo.
* `make ent/generate`
  * writes the output to `ent/` -- all the files except those in `ent/schema/` and `ent/generate.go` are dyamically generated.  do not commit them to the repo.
* `make wren/build`
  * generates the wren executable

## Testing Wren

### Generating Mocks

#### Adding
To add mocks for additional interfaces in wren, update the `.mockery.yaml` file and then run `make wren/generate-mocks` from the root folder of the monorepo.

#### Updating
To update the existing mocks for recent interface changes, run `make wren/generate-mocks` from the from the root folder of the monorepo.

### Running Unit Tests

From the root of the monorepo, run `make wren/test`

## Standards

The following standards are used in the design of the Wren API:

* For gRPC/protobuf:
  * [Google API Design Guide](https://cloud.google.com/apis/design)
  * [Protobuf Style Guide](https://docs.buf.build/style-guide/)
* For REST:
  * [OpenAPI Specification](https://spec.openapis.org/oas/v3.1.0)

## Tools Used

* For protobuf linting / compiling / documentation: [buf](https://docs.buf.build/)
* For the REST reverse-proxy & documentation: [grpc-gateway](https://grpc-ecosystem.github.io/grpc-gateway/)
* For gRPC/protobuf documentation, one of the following (to be decided):
  * [protoc-gen-template](https://github.com/kerinin/protoc-gen-template)
  * [protoc-gen-doc](https://github.com/pseudomuto/protoc-gen-doc)

## Helpful Links/Repos

* [Github googleapis/googleapis](https://github.com/googleapis/googleapis)
* [Github protocolbuffers/protobuf](https://github.com/protocolbuffers/protobuf)


* [Awesome gRPC](https://github.com/grpc-ecosystem/awesome-grpc)
* [gRPC Website](https://grpc.io/) - Official documentation, libraries, resources, samples and FAQ
* [gRPC Technical documentation](https://github.com/grpc/grpc/tree/master/doc) - Collection of useful technical documentation
* [gRPC status codes](https://github.com/grpc/grpc/blob/master/doc/statuscodes.md) - Status codes and their use in gRPC
* [gRPC status code mapping](https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md) - HTTP to gRPC Status Code Mapping
* [grpc-errors](https://github.com/avinassh/grpc-errors) - Code examples in each language on how to return and handle error statuses.
*[Zerolog](https://github.com/rs/zerolog)
