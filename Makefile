PROJECT ?= autobet-470818
REGION ?= europe-west2
REPO ?= autobet-services

# Service names deployed by Terraform / Cloud Run
SRV_FETCHER ?= ingestion-fetcher
SRV_ORCH ?= ingestion-orchestrator
SRV_SUPERFECTA ?= superfecta-trainer

# Image tag defaults to timestamp + git sha for immutability
TAG ?= $(shell date +%Y%m%d%H%M%S)-$(shell git rev-parse --short HEAD)

IMAGE_FETCHER := $(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_FETCHER):$(TAG)
IMAGE_ORCH := $(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_ORCH):$(TAG)
IMAGE_SUPERFECTA := $(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_SUPERFECTA):$(TAG)

ML_BQ_PROJECT ?= $(PROJECT)
ML_BQ_DATASET ?= autobet
ML_BQ_MODEL_DATASET ?= autobet_model
ML_JOB_ARGS ?= scripts/train_superfecta_model.py,--countries,GB,IE,--window-start-minutes,-180,--window-end-minutes,720
RUN_JOB_SA ?=

.PHONY: help
help:
	@echo "Targets:"
	@echo "  enable-apis                Enable required Google Cloud APIs"
	@echo "  create-repo               Create Artifact Registry repo ($(REPO))"
	@echo "  build-and-push-gcb        Build + push service images via Cloud Build (tag: $(TAG))"
	@echo "  deploy-superfecta-job     Deploy/update the Superfecta trainer Cloud Run Job"
	@echo "  terraform-init            Init Terraform in sports/"
	@echo "  terraform-plan            Plan Terraform with project/region vars"
	@echo "  terraform-apply           Apply Terraform with project/region vars"
	@echo "  set-images                Update Cloud Run services to the new images (uses TAG/.last_tag)"
	@echo "  set-images-latest         Update Cloud Run services to the latest pushed digests in Artifact Registry"
	@echo "  deploy                    Full flow: enable-apis, create-repo, build, terraform apply, set-images"

.PHONY: enable-apis
enable-apis:
	gcloud services enable \
	  run.googleapis.com \
	  pubsub.googleapis.com \
	  cloudscheduler.googleapis.com \
	  secretmanager.googleapis.com \
	  artifactregistry.googleapis.com \
	  bigquery.googleapis.com \
	  bigquerystorage.googleapis.com \
	  --project "$(PROJECT)"

.PHONY: create-repo
create-repo:
	gcloud artifacts repositories create "$(REPO)" \
	  --location="$(REGION)" \
	  --repository-format=docker \
	  --description="Autobet services" \
	  --project "$(PROJECT)" || true
	gcloud auth configure-docker "$(REGION)-docker.pkg.dev" --quiet

.PHONY: build-and-push-gcb
build-and-push-gcb:
	@echo "Building images via Cloud Build (source: parent dir):"
	@echo "  FETCHER:        $(IMAGE_FETCHER)"
	@echo "  ORCHESTRATOR:   $(IMAGE_ORCH)"
	@echo "  SUPERFECTA JOB: $(IMAGE_SUPERFECTA)"
	# Submit the parent directory so the workspace contains the 'autobet/' folder.
	# This keeps Dockerfile paths like autobet/sports/Dockerfile.* valid and preserves
	# the autobet.sports.* import path inside the container.
	gcloud builds submit .. \
	  --config sports/cloudbuild.yaml \
	  --substitutions _IMAGE_FETCHER="$(IMAGE_FETCHER)",_IMAGE_ORCHESTRATOR="$(IMAGE_ORCH)",_IMAGE_SUPERFECTA="$(IMAGE_SUPERFECTA)" \
	  --project "$(PROJECT)"
	@echo "$(TAG)" > .last_tag
	@echo "Saved tag to .last_tag: $(TAG)"

.PHONY: terraform-init
terraform-init:
	cd sports && terraform init

.PHONY: terraform-plan
terraform-plan:
	cd sports && terraform plan -var "project_id=$(PROJECT)" -var "region=$(REGION)"

.PHONY: terraform-apply
terraform-apply:
	cd sports && terraform apply -auto-approve -var "project_id=$(PROJECT)" -var "region=$(REGION)"

.PHONY: set-images
set-images:
	@echo "Resolving digests and updating Cloud Run services..."
	# Determine effective TAG: prefer env TAG, else .last_tag, else current computed TAG
	@TAG_EFF=$${TAG:-$$(cat .last_tag 2>/dev/null || echo "$(TAG)")}; \
	FETCHER_TAGGED="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_FETCHER):$$TAG_EFF"; \
	ORCH_TAGGED="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_ORCH):$$TAG_EFF"; \
	FETCHER_DIGEST=$$(gcloud artifacts docker images describe "$$FETCHER_TAGGED" --format='value(image_summary.digest)' 2>/dev/null || true); \
	if [ -n "$$FETCHER_DIGEST" ]; then \
	  FETCHER_IMAGE="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_FETCHER)@$$FETCHER_DIGEST"; \
	else \
	  FETCHER_IMAGE="$$FETCHER_TAGGED"; \
	fi; \
	ORCH_DIGEST=$$(gcloud artifacts docker images describe "$$ORCH_TAGGED" --format='value(image_summary.digest)' 2>/dev/null || true); \
	if [ -n "$$ORCH_DIGEST" ]; then \
	  ORCH_IMAGE="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_ORCH)@$$ORCH_DIGEST"; \
	else \
	  ORCH_IMAGE="$$ORCH_TAGGED"; \
	fi; \
	echo "Updating $(SRV_FETCHER) -> $$FETCHER_IMAGE"; \
	gcloud run services update "$(SRV_FETCHER)" --region "$(REGION)" --project "$(PROJECT)" --image "$$FETCHER_IMAGE"; \
	echo "Updating $(SRV_ORCH) -> $$ORCH_IMAGE"; \
	gcloud run services update "$(SRV_ORCH)" --region "$(REGION)" --project "$(PROJECT)" --image "$$ORCH_IMAGE"

.PHONY: deploy
deploy: enable-apis create-repo build-and-push-gcb terraform-init terraform-apply set-images
	@echo "Deployment complete."

.PHONY: deploy-superfecta-job
deploy-superfecta-job:
	@TAG_EFF=$${TAG:-$$(cat .last_tag 2>/dev/null || echo "$(TAG)")}; \
	IMAGE_TAGGED="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_SUPERFECTA):$$TAG_EFF"; \
	IMAGE_DIGEST=$$(gcloud artifacts docker images describe "$$IMAGE_TAGGED" --project "$(PROJECT)" --format='value(image_summary.digest)' 2>/dev/null || true); \
	if [ -n "$$IMAGE_DIGEST" ]; then \
	  IMAGE_FQ="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_SUPERFECTA)@$$IMAGE_DIGEST"; \
	else \
	  IMAGE_FQ="$$IMAGE_TAGGED"; \
	fi; \
	echo "Deploying Cloud Run Job $(SRV_SUPERFECTA) with image $$IMAGE_FQ"; \
	if [ -n "$(RUN_JOB_SA)" ]; then \
	  gcloud run jobs deploy "$(SRV_SUPERFECTA)" \
	    --project "$(PROJECT)" \
	    --region "$(REGION)" \
	    --image "$$IMAGE_FQ" \
	    --memory 1Gi \
	    --cpu 1 \
	    --set-env-vars BQ_PROJECT=$(ML_BQ_PROJECT),BQ_DATASET=$(ML_BQ_DATASET),BQ_MODEL_DATASET=$(ML_BQ_MODEL_DATASET) \
	    --service-account "$(RUN_JOB_SA)" \
	    --command python \
	    --args "$(ML_JOB_ARGS)"; \
	else \
	  gcloud run jobs deploy "$(SRV_SUPERFECTA)" \
	    --project "$(PROJECT)" \
	    --region "$(REGION)" \
	    --image "$$IMAGE_FQ" \
	    --memory 1Gi \
	    --cpu 1 \
	    --set-env-vars BQ_PROJECT=$(ML_BQ_PROJECT),BQ_DATASET=$(ML_BQ_DATASET),BQ_MODEL_DATASET=$(ML_BQ_MODEL_DATASET) \
	    --command python \
	    --args "$(ML_JOB_ARGS)"; \
	fi
	@echo "Cloud Run Job $(SRV_SUPERFECTA) deployed."

.PHONY: secrets-push
secrets-push:
	python scripts/push_secrets_from_env.py --project "$(PROJECT)" --secrets TOTE_API_KEY TOTE_GRAPHQL_URL

.PHONY: set-images-latest
set-images-latest:
	@echo "Finding latest pushed digests in Artifact Registry and updating services..."
	# Fetch latest digests by UPDATE_TIME for each image path
	@FETCHER_DIGEST=$$(gcloud artifacts docker images list "$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_FETCHER)" --include-tags --format='csv[no-heading](DIGEST,UPDATE_TIME)' | sort -t, -k2,2r | head -n1 | cut -d, -f1); \
	if [ -z "$$FETCHER_DIGEST" ]; then echo "No images found for $(SRV_FETCHER). Did you push?" >&2; exit 2; fi; \
	ORCH_DIGEST=$$(gcloud artifacts docker images list "$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_ORCH)" --include-tags --format='csv[no-heading](DIGEST,UPDATE_TIME)' | sort -t, -k2,2r | head -n1 | cut -d, -f1); \
	if [ -z "$$ORCH_DIGEST" ]; then echo "No images found for $(SRV_ORCH). Did you push?" >&2; exit 2; fi; \
	FETCHER_IMAGE="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_FETCHER)@$$FETCHER_DIGEST"; \
	ORCH_IMAGE="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_ORCH)@$$ORCH_DIGEST"; \
	echo "Updating $(SRV_FETCHER) -> $$FETCHER_IMAGE"; \
	gcloud run services update "$(SRV_FETCHER)" --region "$(REGION)" --project "$(PROJECT)" --image "$$FETCHER_IMAGE"; \
	echo "Updating $(SRV_ORCH) -> $$ORCH_IMAGE"; \
	gcloud run services update "$(SRV_ORCH)" --region "$(REGION)" --project "$(PROJECT)" --image "$$ORCH_IMAGE"
