this chart installs kaskada as a single pod, with both the manager and the engine as containers in that pod.

this doesn't require a "real" object store.  local is okay, and probably should be the default.

also doesn't require a "real" db.  local is okay, and probably should be the default.  even local memory-based as default.

# kaskada-canary

[Kaskada](https://github.com/kaskada-ai/kaskada/) is a query engine for event-based (timestamped) data.

This helm chart installs a persistant version of the kaskada service in the simplest configuration possible.  
This chart should be primarily used for initial testing and not for production scenarios.

In this setup, the kaskada service runs as two containers in a single pod, and persists its database
as a file on attached storage.  It also requires access to an object store.

## Chart Repo

Add the following repo to use the chart:

```console
helm repo add kaskada https://kaskada-ai.github.io/kaskada/
```

## Values

The following table lists the configurable parameters of the _Kaskada-Canary_ chart and their default values.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| commonLabels | object | `{}` | Labels to apply to all resources |
| extraEnv | list | `[]` | Environment variables to add to the pod |
| extraEnvFrom | list | `[]` | Environment variables from secrets or configmaps to add to the pod |
| fullnameOverride | string | `""` | Overrides the chart's computed fullname |
| image.pullPolicy | string | `"IfNotPresent"` | Docker image pull policy |
| image.repository | string | `"ghcr.io/kaskada-ai/engine"` | Docker image repository |
| image.tag | string | `nil` | Overrides the image tag whose default is the chart's appVersion |
| imagePullSecrets | list | `[]` | Image pull secrets for Docker images |
| lokiAddress | string | `nil` | The Loki server URL:Port, e.g. loki:3100 |
| nameOverride | string | `""` | Overrides the chart's name |
| namespace.name | string | `nil` | The name of the Namespace to deploy If not set, `.Release.Namespace` is used |
| nodeSelector | object | `{}` | Node selector for the pod |
| podAnnotations | object | `{}` | Annotations to add to the pod |
| podLabels | object | `{}` | Labels to add to the pod |
| resources.engine | object | `{}` | Resource requests and limits for the engine container |
| resources.manager | object | `{}` | Resource requests and limits for the mananger container |
| service.annotations | object | `{}` | Annotations to add to the service |
| service.appProtocol | string | `""` | Adds the appProtocol field to the service. This allows to work with istio protocol selection. Ex: "http" or "tcp" |
| service.create | bool | `true` | Specifies whether a Service should be created |
| service.grpcPort | int | 50051 | The port for exposing the gRPC service |
| service.labels | object | `{}` | Labels to add to the service |
| service.restPort | int | 3365 | The port for exposing the REST service |
| service.type | string | `"ClusterIP"` | The type of service to create |
| serviceAccount.annotations | object | `{}` | Annotations for the service account |
| serviceAccount.automountServiceAccountToken | bool | `true` | Set this toggle to false to opt out of automounting API credentials for the service account |
| serviceAccount.create | bool | `true` | Specifies whether a ServiceAccount should be created |
| serviceAccount.imagePullSecrets | list | `[]` | Image pull secrets for the service account |
| serviceAccount.name | string | `nil` | The name of the ServiceAccount to use. If not set and create is true, a name is generated using the fullname template |
| storage.objectStore.bucket | string | `nil` | The name of the bucket or container to use for storing data |
| storage.objectStore.path | string | `/` | The bucket path prefix to use for storing data |
| storage.objectStore.type | string | `nil` | The type of object storage to use for the service. Either `s3`, `gcs`, or `azure` |
| storage.objectStore.s3 | object | `{}` | If using objectStore.type `s3`, pass authorization details for accessing s3 here |
| storage.objectStore.gcs | object | `{}` | If using objectStore.type `gcs`, pass authorization details for accessing gcs here |
| storage.objectStore.azure | object | `{}` | If using objectStore.type `azure`, pass authorization details for accessing azure blob storage here |
| storage.dbData.storage | string | `10Gi` | The size of the database volume |
| storage.dbData.storageClassName | string | "" | The type of storage used to provision the persisted database volume. Uses the default storage class if not set |
| storage.tmpData.storage | string | `100Gi` | The size of the temporary volume |
| storage.tmpData.storageClassName | string | "" | The type of storage used to provision the persisted temporary volume. Uses the default storage class if not set |
| tolerations | list | `[]` | Tolerations for the pod |  
