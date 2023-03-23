## Kaskada with Pulsar

Kaskada supports integration with [Pulsar](https://pulsar.apache.org/), a distributed messaging and streaming platform. Query results can be materialized to Pulsar Topics and connected to feature stores. 

### Demo

The following instructions can be used to build a docker image with a standalone pulsar cluster that demonstrates query results being incrementally materialized to a topic. 

* From this directory, run docker compose: `docker compose up -d`
* Open the notebook in a browser
  * Query the `kaskada-jupyter` container for the notebook link: `docker logs kaskada-jupyter`
* Open the `notebooks` directory and select an example!

