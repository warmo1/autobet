PROJECT ?= autobet-470818
REGION ?= europe-west2
REPO ?= autobet-services

# Service names deployed by Terraform
SRV_FETCHER ?= ingestion-fetcher
SRV_ORCH ?= ingestion-orchestrator

# Image tag defaults to timestamp + git sha for immutability
TAG ?= $(shell date +%Y%m%d%H%M%S)-$(shell git rev-parse --short HEAD)

IMAGE_FETCHER := $(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_FETCHER):$(TAG)
IMAGE_ORCH := $(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_ORCH):$(TAG)

.PHONY: help
help:
	@echo "Targets:"
	@echo "  enable-apis                Enable required Google Cloud APIs"
	@echo "  create-repo               Create Artifact Registry repo ($(REPO))"
	@echo "  build-and-push-gcb        Build + push both images via Cloud Build (tag: $(TAG))"
	@echo "  terraform-init            Init Terraform in sports/"
	@echo "  terraform-plan            Plan Terraform with project/region vars"
	@echo "  terraform-apply           Apply Terraform with project/region vars"
	@echo "  set-images                Update Cloud Run services to the new images (pin by digest if available)"
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
	@echo "Building images via Cloud Build:"
	@echo "  FETCHER:     $(IMAGE_FETCHER)"
	@echo "  ORCHESTRATOR:$(IMAGE_ORCH)"
	gcloud builds submit \
	  --config sports/cloudbuild.yaml \
	  --substitutions _IMAGE_FETCHER="$(IMAGE_FETCHER)",_IMAGE_ORCHESTRATOR="$(IMAGE_ORCH)" \
	  --project "$(PROJECT)"

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
	@FETCHER_DIGEST=$$(gcloud artifacts docker images describe "$(IMAGE_FETCHER)" --format='value(image_digest)' 2>/dev/null || true); \
	if [ -n "$$FETCHER_DIGEST" ]; then \
	  FETCHER_IMAGE="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_FETCHER)@$$FETCHER_DIGEST"; \
	else \
	  FETCHER_IMAGE="$(IMAGE_FETCHER)"; \
	fi; \
	ORCH_DIGEST=$$(gcloud artifacts docker images describe "$(IMAGE_ORCH)" --format='value(image_digest)' 2>/dev/null || true); \
	if [ -n "$$ORCH_DIGEST" ]; then \
	  ORCH_IMAGE="$(REGION)-docker.pkg.dev/$(PROJECT)/$(REPO)/$(SRV_ORCH)@$$ORCH_DIGEST"; \
	else \
	  ORCH_IMAGE="$(IMAGE_ORCH)"; \
	fi; \
	echo "Updating $(SRV_FETCHER) -> $$FETCHER_IMAGE"; \
	gcloud run services update "$(SRV_FETCHER)" --region "$(REGION)" --project "$(PROJECT)" --image "$$FETCHER_IMAGE"; \
	echo "Updating $(SRV_ORCH) -> $$ORCH_IMAGE"; \
	gcloud run services update "$(SRV_ORCH)" --region "$(REGION)" --project "$(PROJECT)" --image "$$ORCH_IMAGE"

.PHONY: deploy
deploy: enable-apis create-repo build-and-push-gcb terraform-init terraform-apply set-images
	@echo "Deployment complete."

.PHONY: secrets-push
secrets-push:
	python scripts/push_secrets_from_env.py --project "$(PROJECT)" --secrets TOTE_API_KEY TOTE_GRAPHQL_URL

