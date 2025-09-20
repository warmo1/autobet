# Pub/Sub topic for ingestion jobs
resource "google_pubsub_topic" "ingest_jobs_topic" {
  name = "ingest-jobs"
}

# Real-time WebSocket event topics
resource "google_pubsub_topic" "tote_pool_total_changed" {
  name = "tote-pool-total-changed"
}

resource "google_pubsub_topic" "tote_product_status_changed" {
  name = "tote-product-status-changed"
}

resource "google_pubsub_topic" "tote_event_status_changed" {
  name = "tote-event-status-changed"
}

resource "google_pubsub_topic" "tote_event_result_changed" {
  name = "tote-event-result-changed"
}

resource "google_pubsub_topic" "tote_pool_dividend_changed" {
  name = "tote-pool-dividend-changed"
}

resource "google_pubsub_topic" "tote_selection_status_changed" {
  name = "tote-selection-status-changed"
}

resource "google_pubsub_topic" "tote_competitor_status_changed" {
  name = "tote-competitor-status-changed"
}

resource "google_pubsub_topic" "tote_bet_lifecycle" {
  name = "tote-bet-lifecycle"
}

# Pub/Sub push subscription to the fetcher service
resource "google_pubsub_subscription" "fetcher_subscription" {
  name  = "ingest-fetcher-sub"
  topic = google_pubsub_topic.ingest_jobs_topic.name

  ack_deadline_seconds = 600 # Allow long-running ingest tasks

  push_config {
    push_endpoint = google_cloud_run_v2_service.fetcher_service.uri

    oidc_token {
      service_account_email = google_service_account.fetcher_sa.email
    }
  }

  retry_policy {
    minimum_backoff = "10s"
  }

  # Allow the Pub/Sub service account to invoke the Cloud Run service
  depends_on = [google_cloud_run_v2_service_iam_binding.fetcher_invoker]
}