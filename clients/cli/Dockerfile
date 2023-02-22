FROM ubuntu:latest as ubuntu
RUN apt-get update -y && apt-get install -y ca-certificates

FROM scratch
COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY cli /usr/bin/cli
ENTRYPOINT ["/usr/bin/cli"]
