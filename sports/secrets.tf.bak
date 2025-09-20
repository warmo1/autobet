// Secret Manager IAM bindings for Cloud Run service accounts
// Grants accessor to required secrets used as env vars in services.tf

resource "google_secret_manager_secret_iam_member" "tote_api_key_fetcher" {
  project   = var.project_id
  secret_id = "TOTE_API_KEY"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.fetcher_sa.email}"
}

resource "google_secret_manager_secret_iam_member" "tote_api_key_orchestrator" {
  project   = var.project_id
  secret_id = "TOTE_API_KEY"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}

resource "google_secret_manager_secret_iam_member" "tote_graphql_url_fetcher" {
  project   = var.project_id
  secret_id = "TOTE_GRAPHQL_URL"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.fetcher_sa.email}"
}

resource "google_secret_manager_secret_iam_member" "tote_graphql_url_orchestrator" {
  project   = var.project_id
  secret_id = "TOTE_GRAPHQL_URL"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}

