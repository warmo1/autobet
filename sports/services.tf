# Service Account for the fetcher service
resource "google_service_account" "fetcher_sa" {
  account_id   = var.service_name_fetcher
  display_name = "Service Account for Ingestion Fetcher"
}

# Service Account for the orchestrator service
resource "google_service_account" "orchestrator_sa" {
  account_id   = var.service_name_orchestrator
  display_name = "Service Account for Ingestion Orchestrator"
}

# Service Account for the WebSocket service
resource "google_service_account" "websocket_sa" {
  account_id   = var.service_name_websocket
  display_name = "Service Account for WebSocket Subscription Service"
}

# IAM bindings for service accounts
resource "google_project_iam_member" "fetcher_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.fetcher_sa.email}"
}
resource "google_project_iam_member" "fetcher_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.fetcher_sa.email}"
}

resource "google_project_iam_member" "orchestrator_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}
resource "google_project_iam_member" "orchestrator_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}
resource "google_project_iam_member" "orchestrator_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}
resource "google_project_iam_member" "orchestrator_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}

# IAM bindings for WebSocket service account
resource "google_project_iam_member" "websocket_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.websocket_sa.email}"
}
resource "google_project_iam_member" "websocket_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.websocket_sa.email}"
}
resource "google_project_iam_member" "websocket_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.websocket_sa.email}"
}

# Cloud Run Service: ingestion-fetcher
resource "google_cloud_run_v2_service" "fetcher_service" {
  name     = var.service_name_fetcher
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL" # Required for Pub/Sub push
  deletion_protection = false

  template {
    service_account = google_service_account.fetcher_sa.email
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_name}/${var.service_name_fetcher}:latest"
      ports {
        container_port = 8080
      }
      # Env vars: non-sensitive as literals; sensitive via Secret Manager
      env {
        name  = "BQ_PROJECT"
        value = var.project_id
      }
      env {
        name  = "BQ_DATASET"
        value = var.bq_dataset
      }
      env {
        name  = "BQ_LOCATION"
        value = var.bq_location
      }
      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }
      env {
        name  = "CLOUD_RUN_REGION"
        value = var.region
      }
      env {
        name  = "BQ_WRITE_ENABLED"
        value = "1"
      }
      env {
        name = "TOTE_API_KEY"
        value_source {
          secret_key_ref {
            secret  = "TOTE_API_KEY"
            version = "latest"
          }
        }
      }
      env {
        name = "TOTE_GRAPHQL_URL"
        value_source {
          secret_key_ref {
            secret  = "TOTE_GRAPHQL_URL"
            version = "latest"
          }
        }
      }
      resources {
        limits = {
          cpu    = "1000m"
          memory = "512Mi"
        }
      }
    }
  }
}

# Cloud Run Service: ingestion-orchestrator
resource "google_cloud_run_v2_service" "orchestrator_service" {
  name     = var.service_name_orchestrator
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL" # Required for Cloud Scheduler
  deletion_protection = false

  template {
    service_account = google_service_account.orchestrator_sa.email
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_name}/${var.service_name_orchestrator}:latest"
      ports {
        container_port = 8080
      }
      env {
        name  = "BQ_PROJECT"
        value = var.project_id
      }
      env {
        name  = "BQ_DATASET"
        value = var.bq_dataset
      }
      env {
        name  = "BQ_LOCATION"
        value = var.bq_location
      }
      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }
      env {
        name  = "CLOUD_RUN_REGION"
        value = var.region
      }
      env {
        name = "TOTE_API_KEY"
        value_source {
          secret_key_ref {
            secret  = "TOTE_API_KEY"
            version = "latest"
          }
        }
      }
      env {
        name = "TOTE_GRAPHQL_URL"
        value_source {
          secret_key_ref {
            secret  = "TOTE_GRAPHQL_URL"
            version = "latest"
          }
        }
      }
      resources {
        limits = {
          cpu    = "1000m"
          memory = "512Mi"
        }
      }
    }
  }
}
