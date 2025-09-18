# Scripted Cloud Deployment with Terraform

This guide outlines how to use the provided `Makefile` and Terraform scripts to deploy the entire data ingestion pipeline to Google Cloud. This approach automates the creation of all necessary resources, ensuring a consistent and error-free setup.

## Architecture Overview

1.  **Cloud Scheduler**: Acts as the primary trigger for our pipelines. We'll create three types of jobs.
2.  **Pub/Sub Topic (`ingest-jobs`)**: A message queue that decouples the schedulers from the services that do the work.
3.  **Cloud Run Service (`ingestion-orchestrator`)**: A "smart" service triggered by high-level scheduler jobs. It queries BigQuery to find out *what* needs to be refreshed (e.g., races starting soon) and publishes more specific messages back to the Pub/Sub topic.
4.  **Cloud Run Service (`ingestion-fetcher`)**: A "worker" service that subscribes to the Pub/Sub topic. It receives specific tasks (e.g., "fetch product X") and executes them, ingesting data into BigQuery.

## Prerequisites

1.  **Google Cloud SDK (`gcloud`)**: Install and initialize.
2.  **Terraform**: Install Terraform.
3.  **Docker**: Install Docker.
4.  **Authentication**: Log in to GCP and configure Docker credentials.
    ```bash
    gcloud auth login
    gcloud auth application-default login
    gcloud auth configure-docker europe-west2-docker.pkg.dev
    ```
5.  **Configuration**: Open the `Makefile` in the project root and verify that the `GCP_PROJECT_ID` and `GCP_REGION` variables are set correctly for your environment.

## Deployment Steps

You can deploy the entire stack by running a single command from your laptop's terminal.

```bash
make deploy
```

This command will automatically:
1.  Enable the required Google Cloud APIs.
2.  Create a Google Artifact Registry repository to store your container images.
3.  Build the Docker images for the `ingestion-fetcher` and `ingestion-orchestrator` services.
4.  Push the images to Artifact Registry.
5.  Initialize Terraform.
6.  Apply the Terraform configuration to create all cloud resources:
    -   Service Accounts with correct IAM permissions.
    -   The `ingest-jobs` Pub/Sub topic.
    -   The two Cloud Run services (`ingestion-fetcher` and `ingestion-orchestrator`).
    -   The Pub/Sub push subscription connecting the topic to the fetcher service.
    -   The Cloud Scheduler jobs (daily ingest, scanners, cleanup) with the correct targets and schedules.

## Other Useful Commands

*   **See what will change**: `make terraform-plan`
*   **Only build and push images**: `make build-and-push`
*   **Destroy all deployed resources**: `make terraform-destroy`

With this setup, you have a fully automated, robust, and version-controlled deployment process for your entire ingestion pipeline.
