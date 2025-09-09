# Pub/Sub topic for ingestion jobs
resource "google_pubsub_topic" "ingest_jobs_topic" {
  name = "ingest-jobs"
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