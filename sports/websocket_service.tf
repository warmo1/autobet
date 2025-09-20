# Cloud Run Service: websocket-subscription
resource "google_cloud_run_v2_service" "websocket_service" {
  name     = var.service_name_websocket
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    service_account = google_service_account.websocket_sa.email
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo_name}/${var.service_name_websocket}:latest"
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
      env {
        name = "TOTE_SUBSCRIPTIONS_URL"
        value_source {
          secret_key_ref {
            secret  = "TOTE_SUBSCRIPTIONS_URL"
            version = "latest"
          }
        }
      }
      resources {
        limits = {
          cpu    = "2000m"
          memory = "1Gi"
        }
      }
    }
  }
}
