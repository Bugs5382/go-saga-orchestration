{{- define "gosaga.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "gosaga.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "gosaga.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "gosaga.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "gosaga.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* selectorLabels expects the root scope plus a "component" key (use merge). */}}
{{- define "gosaga.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gosaga.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "gosaga.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "gosaga.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "gosaga.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{/* Connection env (store + rabbitmq) sourced from connectionSecret. Both
     services consume this; keys are optional so only the ones your backend
     needs must exist. RABBITMQ_URL is required at runtime by both services. */}}
{{- define "gosaga.connEnv" -}}
- name: DATABASE_DSN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.connectionSecret }}
      key: {{ .Values.connectionSecretKeys.databaseDsn }}
      optional: true
- name: REDIS_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.connectionSecret }}
      key: {{ .Values.connectionSecretKeys.redisUrl }}
      optional: true
- name: RABBITMQ_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.connectionSecret }}
      key: {{ .Values.connectionSecretKeys.rabbitmqUrl }}
      optional: true
{{- end -}}
