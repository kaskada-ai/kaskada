###############################################################################
# Create the rust-builder

FROM rust:slim-buster AS rust-sysdeps
# needs perl to install and configure openssl
RUN apt-get update && \
    apt-get install \
    bc \
    clang \
    libssl-dev \
    lld \
    perl \
    pkg-config \
    make \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    --no-install-recommends \
    -y && \
    rm -rf /var/lib/lapt/lists/*

# Setup default (stable) toolchain first in a separate layer (since it is
# less likely to change / more likely to be cached)
RUN rustup update && \
    rustup component add clippy llvm-tools-preview rustfmt
RUN cargo install sccache
RUN echo "[build]\nrustc-wrapper = \"/usr/local/cargo/bin/sccache\""

FROM rust-sysdeps as rust-prepare
RUN cargo install grcov \
    && RUST_BACKTRACE=full cargo install cargo-deny

FROM rust-sysdeps as rust-builder
COPY --from=rust-prepare /usr/local/cargo/bin/grcov /usr/local/cargo/bin/grcov
COPY --from=rust-prepare /usr/local/cargo/bin/cargo-deny /usr/local/cargo/bin/cargo-deny
COPY --from=rust-prepare /usr/local/cargo/bin/sccache /usr/local/cargo/bin/sccache

###############################################################################
# Create the runtime environment

FROM debian:buster-slim as runtime

RUN  apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates wget \
    && update-ca-certificates

# Install grpc-health-probe binary into container
RUN GRPC_HEALTH_PROBE_VERSION=v0.4.11 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

###############################################################################
# Build the sparrow code

FROM rust-builder as sparrow-build

COPY ./*.toml  /builds/kaskada/
COPY ./*.lock  /builds/kaskada/
COPY ./NOTICE  /builds/kaskada/
COPY ./crates/ /builds/kaskada/crates/
COPY ./proto/  /builds/kaskada/proto/
WORKDIR /builds/kaskada

# outputs to ./target/debug/sparrow-main
RUN cargo build --bin sparrow-main

###############################################################################
# Build the wren code

FROM golang:1.19 AS wren-build

RUN mkdir -p /builds/kaskada/wren
WORKDIR /builds/kaskada/wren

# Fetch dependencies.
COPY ./go.mod /builds/kaskada/
COPY ./go.sum /builds/kaskada/
RUN go mod download

#compile minimal executable
COPY NOTICE /builds/kaskada/wren/
COPY ./wren /builds/kaskada/wren
RUN go build -ldflags="-w -s" -o /go/bin/wren main.go

###############################################################################
# create the final container

FROM runtime

COPY --from=sparrow-build /builds/kaskada/target/debug/sparrow-main /bin/sparrow-main
COPY --from=wren-build /go/bin/wren /bin/wren
COPY run.sh run.sh

CMD ["./run.sh"]
