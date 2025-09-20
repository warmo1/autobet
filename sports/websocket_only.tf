# WebSocket Architecture - Only new resources

# Service Account for the WebSocket service
resource "google_service_account" "websocket_sa" {
  account_id   = var.service_name_websocket
  display_name = "Service Account for WebSocket Subscription Service"
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

# WebSocket service
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
          cpu    = "1000m"
          memory = "512Mi"
        }
      }
    }
  }
}

# WebSocket scheduler jobs
resource "google_cloud_scheduler_job" "websocket_start" {
  name             = "websocket-start"
  description      = "Start WebSocket subscription service during peak hours"
  schedule         = "0 6 * * *"
  time_zone        = "Europe/London"
  attempt_deadline = "60s"

  http_target {
    uri         = "${google_cloud_run_v2_service.websocket_service.uri}/start"
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode("{}")
    oidc_token {
      service_account_email = "autobet-scheduler@autobet-470818.iam.gserviceaccount.com"
    }
  }
}

resource "google_cloud_scheduler_job" "websocket_stop" {
  name             = "websocket-stop"
  description      = "Stop WebSocket subscription service after peak hours"
  schedule         = "0 23 * * *"
  time_zone        = "Europe/London"
  attempt_deadline = "60s"

  http_target {
    uri         = "${google_cloud_run_v2_service.websocket_service.uri}/stop"
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode("{}")
    oidc_token {
      service_account_email = "autobet-scheduler@autobet-470818.iam.gserviceaccount.com"
    }
  }
}

# Simple dashboard for WebSocket monitoring
resource "google_monitoring_dashboard" "websocket_dashboard" {
  dashboard_json = jsonencode({
    displayName = "WebSocket Architecture Monitoring"
    mosaicLayout = {
      columns = 12
      tiles = [
        {
          width  = 6
          height = 4
          widget = {
            title = "WebSocket Service Health"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription"
                      aggregation = {
                        alignmentPeriod    = "300s"
                        perSeriesAligner   = "ALIGN_RATE"
                        crossSeriesReducer = "REDUCE_SUM"
                      }
                    }
                  }
                  plotType = "LINE"
                }
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "Requests/sec"
                scale = "LINEAR"
              }
            }
          }
        },
        {
          width  = 6
          height = 4
          xPos   = 6
          widget = {
            title = "WebSocket Errors"
            xyChart = {
              dataSets = [
                {
                  timeSeriesQuery = {
                    timeSeriesFilter = {
                      filter = "resource.type=cloud_run_revision AND resource.labels.service_name=websocket-subscription AND severity>=ERROR"
                      aggregation = {
                        alignmentPeriod    = "300s"
                        perSeriesAligner   = "ALIGN_RATE"
                        crossSeriesReducer = "REDUCE_SUM"
                      }
                    }
                  }
                  plotType = "LINE"
                }
              ]
              timeshiftDuration = "0s"
              yAxis = {
                label = "Errors/sec"
                scale = "LINEAR"
              }
            }
          }
        }
      ]
    }
  })
}
