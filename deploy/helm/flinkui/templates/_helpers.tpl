{{/*
Expand the name of the chart.
*/}}
{{- define "flinkui.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name.
*/}}
{{- define "flinkui.fullname" -}}
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

{{- define "flinkui.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "flinkui.labels" -}}
helm.sh/chart: {{ include "flinkui.chart" . }}
{{ include "flinkui.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "flinkui.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flinkui.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "flinkui.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "flinkui.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Namespace that holds the FlinkDeployment resources (RBAC target).
Defaults to the release namespace.
*/}}
{{- define "flinkui.targetNamespace" -}}
{{- default .Release.Namespace .Values.config.targetNamespace }}
{{- end }}

{{/*
Name of the auth Secret (created or existing).
*/}}
{{- define "flinkui.authSecretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "flinkui.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Name of the S3 Secret (created or existing).
*/}}
{{- define "flinkui.s3SecretName" -}}
{{- if .Values.s3.existingSecret }}
{{- .Values.s3.existingSecret }}
{{- else }}
{{- printf "%s-s3" (include "flinkui.fullname" .) }}
{{- end }}
{{- end }}
