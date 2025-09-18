# IAM binding to allow Cloud Scheduler to invoke the orchestrator service
resource "google_cloud_run_v2_service_iam_binding" "orchestrator_invoker" {
  project  = google_cloud_run_v2_service.orchestrator_service.project
  location = google_cloud_run_v2_service.orchestrator_service.location
  name     = google_cloud_run_v2_service.orchestrator_service.name
  role     = "roles/run.invoker"
  members  = [
    "serviceAccount:${google_service_account.scheduler_sa.email}"
  ]
}

# IAM binding to allow Pub/Sub to invoke the fetcher service
resource "google_cloud_run_v2_service_iam_binding" "fetcher_invoker" {
  project  = google_cloud_run_v2_service.fetcher_service.project
  location = google_cloud_run_v2_service.fetcher_service.location
  name     = google_cloud_run_v2_service.fetcher_service.name
  role     = "roles/run.invoker"
  # This uses a special service account managed by Google for Pub/Sub push subscriptions
  members  = [
    # OIDC token is signed as the fetcher service account in subscription push_config
    "serviceAccount:${google_service_account.fetcher_sa.email}",
    # Keep Pub/Sub service agent as invoker too (defensive)
    "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  ]
}

# Service account for Cloud Scheduler jobs
resource "google_service_account" "scheduler_sa" {
  account_id   = "autobet-scheduler"
  display_name = "Service Account for Autobet Cloud Scheduler"
}

# Data source to get the project number
data "google_project" "project" {}

# --- Scheduler Jobs ---

resource "google_cloud_scheduler_job" "daily_full_ingest" {
  name             = "daily-full-ingest"
  description      = "Daily ingest of all products (OPEN, CLOSED, etc.) for the day."
  schedule         = "15 1 * * *" # Every day at 1:15 AM
  time_zone        = "Europe/London"
  attempt_deadline = "300s"

  pubsub_target {
    topic_name = google_pubsub_topic.ingest_jobs_topic.id
    data       = base64encode("{\"task\": \"ingest_products_for_day\", \"date\": \"today\"}")
  }
}

# Allow Pub/Sub service agent to mint OIDC tokens as the fetcher service account
resource "google_service_account_iam_member" "pubsub_can_act_as_fetcher_sa" {
  service_account_id = google_service_account.fetcher_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allow Cloud Scheduler service agent to mint OIDC tokens as the scheduler service account
resource "google_service_account_iam_member" "scheduler_can_act_as_scheduler_sa" {
  service_account_id = google_service_account.scheduler_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
}

# Allow Cloud Scheduler service agent to publish to Pub/Sub topic
resource "google_pubsub_topic_iam_member" "scheduler_publisher" {
  topic  = google_pubsub_topic.ingest_jobs_topic.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
}

resource "google_cloud_scheduler_job" "pre_race_scanner" {
  name             = "pre-race-scanner"
  description      = "Scans for upcoming races every 5 minutes to trigger high-frequency updates."
  schedule         = "*/5 * * * *"
  time_zone        = "UTC"
  attempt_deadline = "120s"

  http_target {
    uri = google_cloud_run_v2_service.orchestrator_service.uri
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode("{\"job_name\": \"pre-race-scanner\"}")
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

resource "google_cloud_scheduler_job" "post_race_results_scanner" {
  name             = "post-race-results-scanner"
  description      = "Scans for recently finished races to ingest results."
  schedule         = "*/15 * * * *"
  time_zone        = "UTC"
  attempt_deadline = "120s"

  http_target {
    uri = google_cloud_run_v2_service.orchestrator_service.uri
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode("{\"job_name\": \"post-race-results-scanner\"}")
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

# Probable odds sweep every 10 minutes (broad window)
resource "google_cloud_scheduler_job" "probable_odds_sweep" {
  name             = "probable-odds-sweep"
  description      = "Publishes probable-odds jobs for upcoming and just-started events."
  schedule         = "*/10 * * * *"
  time_zone        = "UTC"
  attempt_deadline = "120s"

  http_target {
    uri = google_cloud_run_v2_service.orchestrator_service.uri
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode("{\"job_name\": \"probable-odds-sweep\"}")
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

# Daily Tote events ingest trigger (runs once each morning after full ingest)
resource "google_cloud_scheduler_job" "daily_event_ingest" {
  name             = "daily-event-ingest"
  description      = "Publishes a job that ingests today's Tote events."
  schedule         = "30 5 * * *" # Every day at 05:30 Europe/London
  time_zone        = "Europe/London"
  attempt_deadline = "120s"

  http_target {
    uri = google_cloud_run_v2_service.orchestrator_service.uri
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode("{\"job_name\": \"daily-event-ingest\"}")
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

# Weekly cleanup of temporary BigQuery tables (_tmp_*) via Pub/Sub
resource "google_cloud_scheduler_job" "bq_tmp_cleanup" {
  name             = "bq-tmp-cleanup"
  description      = "Deletes leftover _tmp_ tables in BigQuery dataset"
  schedule         = "0 3 * * 0" # Sundays at 03:00
  time_zone        = "Europe/London"
  attempt_deadline = "120s"

  pubsub_target {
    topic_name = google_pubsub_topic.ingest_jobs_topic.id
    data       = base64encode("{\"task\": \"cleanup_bq_temps\", \"older_than_days\": 1}")
  }
}
