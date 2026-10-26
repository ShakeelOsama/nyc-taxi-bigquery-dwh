terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 5.0" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  labels = { project = "nyc-taxi-dwh", managed_by = "terraform" }
  layers = {
    raw      = "Landing copies of source extracts. All STRING, append-only, lineage columns. Data engineers only."
    cleaned  = "Typed, deduplicated, standardised source entities (dbt). Data engineers + analytics engineers."
    business = "Star schema + marts consumed by BI and analysts (dbt)."
  }
}

# ---------- GCS landing zone ----------
resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-nyc-tlc-landing"
  location                    = var.location
  uniform_bucket_level_access = true
  labels                      = local.labels

  lifecycle_rule {
    condition { age = 30 }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition { age = 365 }
    action { type = "Delete" }
  }
}

# ---------- BigQuery datasets: one per layer ----------
resource "google_bigquery_dataset" "layer" {
  for_each    = local.layers
  dataset_id  = each.key
  location    = var.location
  description = each.value
  labels      = merge(local.labels, { layer = each.key })
}

# ---------- Service account used by Airflow / dbt ----------
resource "google_service_account" "pipeline" {
  account_id   = "nyc-taxi-pipeline"
  display_name = "NYC taxi ELT pipeline (ingestion + dbt)"
}

resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_editor" {
  for_each   = google_bigquery_dataset.layer
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_bucket" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# ---------- Analysts: read business layer only ----------
resource "google_bigquery_dataset_iam_member" "analysts_business_reader" {
  count      = var.analyst_group == "" ? 0 : 1
  dataset_id = google_bigquery_dataset.layer["business"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "group:${var.analyst_group}"
}

output "landing_bucket" { value = google_storage_bucket.landing.name }
output "pipeline_service_account" { value = google_service_account.pipeline.email }
