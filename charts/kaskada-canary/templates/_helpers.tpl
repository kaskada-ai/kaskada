{{/*
Expand the name of the chart.
*/}}
{{- define "kaskada-canary.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kaskada-canary.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kaskada-canary.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kaskada-canary.labels" -}}
helm.sh/chart: {{ include "kaskada-canary.chart" . }}
{{ include "kaskada-canary.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{- toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kaskada-canary.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kaskada-canary.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the namespace
*/}}
{{- define "kaskada-canary.namespace" -}}
{{- default .Release.Namespace .Values.namespace }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kaskada-canary.serviceAccountName" -}}
{{- if .Values.auth.serviceAccount.create -}}
    {{ default (include "kaskada-canary.fullname" .) .Values.auth.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.auth.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Shared env vars for aws
*/}}
{{- define "kaskada-canary.awsEnv" -}}
{{- if (eq .Values.storage.objectStore.type "s3") }}
{{- if .Values.auth.aws.accessKeyId }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "kaskada-canary.fullname" . }}
      key: AWS_ACCESS_KEY_ID
{{- end }}
{{- if .Values.auth.aws.secretAccessKey }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "kaskada-canary.fullname" . }}
      key: AWS_SECRET_ACCESS_KEY
{{- end }}
{{- if .Values.auth.aws.region }}
- name: AWS_REGION
  value:  {{ .Values.auth.aws.region }}
- name: AWS_DEFAULT_REGION
  value:  {{ .Values.auth.aws.region }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Env vars for engine logging config
*/}}
{{- define "kaskada-canary.engineLogging" -}}

- name: SPARROW_LOG_FILTER
{{- if (eq .Values.logging.level "debug") }}
  value: "egg::=warn,sparrow_=debug,info"
{{- else }}
  value: "egg::=warn,sparrow_=info,info"
{{- end }}

- name: SPARROW_LOG_JSON
{{- if (eq .Values.logging.format "console") }}
  value: "false"
{{- else }}
  value: "true"
{{- end }}

{{- end -}}

{{/*
Env vars for manager logging config
*/}}
{{- define "kaskada-canary.managerLogging" -}}

- name: DEBUG
{{- if (eq .Values.logging.level "debug") }}
  value: "true"
{{- else }}
  value: "false"
{{- end }}

- name: LOG_FORMAT_JSON
{{- if (eq .Values.logging.format "console") }}
  value: "false"
{{- else }}
  value: "true"
{{- end }}

{{- end -}}
