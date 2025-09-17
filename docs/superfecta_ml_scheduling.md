# Superfecta ML Scheduling

This note explains how to run the `train_superfecta_model.py` script from Cloud Run and trigger it with Cloud Scheduler.

## 1. Build a Cloud Run job image

```bash
# from repo root
make build-and-push-gcb TAG=$(date +%Y%m%d%H%M%S)
make deploy-superfecta-job
```

The Make targets will:

1. Build all three containers (fetcher, orchestrator, superfecta job) via Cloud Build.
2. Push them to Artifact Registry with the chosen `TAG` (written to `.last_tag`).
3. Deploy/refresh the Cloud Run Job `superfecta-trainer` with the latest image and default arguments (`--countries GB IE --window-start-minutes -180 --window-end-minutes 720`).

Optional overrides before running `make deploy-superfecta-job`:

| variable | purpose | default |
|----------|---------|---------|
| `ML_BQ_PROJECT` | BigQuery project | `$(PROJECT)` |
| `ML_BQ_DATASET` | Feature dataset | `autobet` |
| `ML_BQ_MODEL_DATASET` | Predictions dataset | `autobet_model` |
| `ML_JOB_ARGS` | Arguments passed to the trainer | `scripts/train_superfecta_model.py,--countries,GB,IE,--window-start-minutes,-180,--window-end-minutes,720` |
| `RUN_JOB_SA` | (optional) Service account email for the job | unset |

## 2. Cloud Scheduler triggers

Create a service account with permission to run the job:

```bash
gcloud iam service-accounts create superfecta-runner --project ${PROJECT}
gcloud projects add-iam-policy-binding ${PROJECT} \
  --member serviceAccount:superfecta-runner@${PROJECT}.iam.gserviceaccount.com \
  --role roles/run.invoker
```

### Daily full refresh (10:00 UTC)

```bash
gcloud scheduler jobs create http superfecta-trainer-daily \
  --project ${PROJECT} \
  --schedule "0 10 * * *" \
  --uri "https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run" \
  --http-method POST \
  --oauth-service-account-email superfecta-runner@${PROJECT}.iam.gserviceaccount.com \
  --location ${REGION}
```

This uses the arguments baked into the job (`--window-start-minutes -180 --window-end-minutes 720`), so it scores the entire day.

### Intraday re-score per race

Schedule a smaller window every 15 minutes during racing hours:

```bash
gcloud scheduler jobs create http superfecta-trainer-intraday \
  --project ${PROJECT} \
  --schedule "*/15 10-23 * * *" \
  --uri "https://run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run" \
  --http-method POST \
  --oauth-service-account-email superfecta-runner@${PROJECT}.iam.gserviceaccount.com \
  --location ${REGION} \
  --message-body '{"args":["--countries","GB","IE","--window-start-minutes","-45","--window-end-minutes","120"]}'
```

Cloud Scheduler sends `message-body` as JSON, which Cloud Run uses to override `args` for this run. The 45-minute back window captures late updates; 120-minute forward window keeps predictions fresh without hammering BigQuery.

## 3. Verifying runs

```bash
gcloud run jobs executions list --project ${PROJECT} --region ${REGION} --job ${JOB_NAME}
gcloud run jobs executions tail --project ${PROJECT} --region ${REGION} --job ${JOB_NAME} --execution EXECUTION_ID
```

Successful executions append rows to `autobet_model.superfecta_runner_predictions`. The new `vw_superfecta_predictions_latest` view powers the ML planner page.

## 4. Adjusting windows per race

If you want to trigger closer to the post time, change the intraday schedule or have Cloud Scheduler run a small Pub/Sub-triggered Cloud Run service that looks up the exact start times (BigQuery query on `vw_gb_open_superfecta_next60`) and calls `gcloud run jobs run`. The tooling above keeps everything declarative so you can tweak schedules without code changes.
