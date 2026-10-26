variable "project_id" {
  type        = string
  description = "GCP project that hosts the warehouse"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "location" {
  type        = string
  default     = "US"
  description = "BigQuery / GCS multi-region"
}

variable "analyst_group" {
  type        = string
  default     = ""
  description = "Google group granted read access to the business dataset (optional)"
}
