# kaskada-canary

[Kaskada](https://github.com/kaskada-ai/kaskada/) is a query engine for event-based (timestamped) data.

This helm chart installs a persistant version of the kaskada service in the simplest configuration possible.  
This chart is primarily for initial testing and should not be used in production scenarios.

In this setup, the kaskada service runs as two containers in a single pod, and persists its database
as a file on attached storage.  It also requires access to an object store.

## Chart Repo

Add the following repo to use the chart:

```console
helm repo add kaskada https://kaskada-ai.github.io/kaskada/
```
