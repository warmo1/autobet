# Pub/Sub subscriptions for real-time events
# These subscriptions are consumed by the web app for SSE updates

resource "google_pubsub_subscription" "tote_pool_total_changed_sub" {
  name  = "tote-pool-total-changed-sub"
  topic = google_pubsub_topic.tote_pool_total_changed.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s" # 10 minutes

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_subscription" "tote_product_status_changed_sub" {
  name  = "tote-product-status-changed-sub"
  topic = google_pubsub_topic.tote_product_status_changed.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_subscription" "tote_event_status_changed_sub" {
  name  = "tote-event-status-changed-sub"
  topic = google_pubsub_topic.tote_event_status_changed.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_subscription" "tote_event_result_changed_sub" {
  name  = "tote-event-result-changed-sub"
  topic = google_pubsub_topic.tote_event_result_changed.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_subscription" "tote_pool_dividend_changed_sub" {
  name  = "tote-pool-dividend-changed-sub"
  topic = google_pubsub_topic.tote_pool_dividend_changed.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_subscription" "tote_selection_status_changed_sub" {
  name  = "tote-selection-status-changed-sub"
  topic = google_pubsub_topic.tote_selection_status_changed.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_subscription" "tote_competitor_status_changed_sub" {
  name  = "tote-competitor-status-changed-sub"
  topic = google_pubsub_topic.tote_competitor_status_changed.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_subscription" "tote_bet_lifecycle_sub" {
  name  = "tote-bet-lifecycle-sub"
  topic = google_pubsub_topic.tote_bet_lifecycle.name

  ack_deadline_seconds = 60
  message_retention_duration = "600s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}
