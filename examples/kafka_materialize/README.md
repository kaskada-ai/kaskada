# Kafka with Schema Registry in Docker Compose

The goal of this readme is document the setup instructions for running Kafka locally to use with Sparrow/Wren/Kaskada. This implementation is designed for streaming reads.

## Setup

Run a `docker compose up -d`

## Details

* Kafka Worker on `kafka1:9092`
* Schema Registry on `schemaregistry:8085`
* Zookeeper on `zookeeper:2181`
* Kafka Rest API on `restproxy:8082`
* Kafka SQL server on `ksql-server:8088`

