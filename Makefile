# Usage:
#   make setup
#   make ingest START=2024-01 END=2024-12      # ~40M trips, downloads ~600 MB of parquet
#   make ingest-public START=2022-01 END=2022-12   # no download: copies from bigquery-public-data
#   make build
GCP_PROJECT ?= $(shell gcloud config get-value project 2>/dev/null)
BUCKET      ?= $(GCP_PROJECT)-nyc-tlc-landing
TARGET      ?= dev
START       ?= 2024-01
END         ?= 2024-03

export GCP_PROJECT

.PHONY: setup infra ingest ingest-public ingest-sandbox build build-sandbox full-refresh test docs

setup:
	pip install -r requirements.txt
	cp -n dbt/profiles.yml.example dbt/profiles.yml || true
	cd dbt && dbt deps

infra:
	cd infra/terraform && terraform init && terraform apply -var="project_id=$(GCP_PROJECT)"

ingest:
	python ingestion/load_tlc_month.py --project $(GCP_PROJECT) --bucket $(BUCKET) --start $(START) --end $(END)

ingest-public:
	python ingestion/load_tlc_month.py --project $(GCP_PROJECT) --from-public-dataset --start $(START) --end $(END)

# BigQuery Sandbox (no billing): unpartitioned raw rebuilt from the public dataset
ingest-sandbox:
	python ingestion/load_tlc_month.py --project $(GCP_PROJECT) --from-public-dataset --sandbox --start $(START) --end $(END)

build:
	cd dbt && dbt seed --target $(TARGET) --profiles-dir . && dbt build --target $(TARGET) --profiles-dir .

# BigQuery Sandbox (no billing/card): no MERGE, so incremental models are rebuilt in full
build-sandbox:
	cd dbt && dbt seed --target $(TARGET) --profiles-dir . && dbt build --target $(TARGET) --profiles-dir . --full-refresh --vars '{sandbox: true}'

full-refresh:
	cd dbt && dbt build --target $(TARGET) --profiles-dir . --full-refresh

test:
	cd dbt && dbt test --target $(TARGET) --profiles-dir .

docs:
	cd dbt && dbt docs generate --target $(TARGET) --profiles-dir . && dbt docs serve --profiles-dir .
