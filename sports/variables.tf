variable "project_id" {
  description = "The GCP project ID to deploy to."
  type        = string
}

variable "region" {
  description = "The GCP region for deploying services."
  type        = string
  default     = "europe-west2"
}

variable "artifact_repo_name" {
  description = "The name of the Artifact Registry repository."
  type        = string
  default     = "autobet-services"
}

variable "service_name_fetcher" {
  description = "Name for the ingestion fetcher Cloud Run service."
  type        = string
  default     = "ingestion-fetcher"
}

variable "service_name_orchestrator" {
  description = "Name for the ingestion orchestrator Cloud Run service."
  type        = string
  default     = "ingestion-orchestrator"
}

variable "service_name_websocket" {
  description = "Name for the WebSocket subscription Cloud Run service."
  type        = string
  default     = "websocket-subscription"
}

variable "bq_dataset" {
  description = "BigQuery dataset name for the app."
  type        = string
  default     = "autobet"
}

variable "bq_location" {
  description = "BigQuery location (e.g., EU, US)."
  type        = string
  default     = "EU"
}
