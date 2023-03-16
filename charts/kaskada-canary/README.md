this chart installs kaskada as a single pod, with both the manager and the engine as containers in that pod.

this doesn't require a "real" object store.  local is okay, and probably should be the default.

also doesn't require a "real" db.  local is okay, and probably should be the default.  even local memory-based as default.

# Kaskada Compute Engine

[Kaskada](https://github.com/kaskada-ai/kaskada/) is a query engine for event-based (timestamped) data.

## Installing the Chart

Before you can install the chart you will need to add the `kaskada` repo to [Helm](https://helm.sh/).

```shell
helm repo add kaskada https://kaskada-ai.github.io/kaskada/
```

After you've installed the repo you can install the chart.

```shell
helm upgrade --install kaskada kaskada/kaskada
```

## Configurations

The following table lists the configurable parameters of the _Kaskada-Canary_ chart and their default values.

| Parameter                          | Description                                                                                                                                                                                                                                                                                                           | Default                                     |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
