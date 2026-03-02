# Main Terraform configuration for WoS XML to BigQuery Dataflow Pipeline
# Orchestrates all modules

terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Backend configuration for state storage
  # Uncomment and configure for production use
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "wos-pipeline"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "dataflow" {
  project = var.project_id
  service = "dataflow.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  project = var.project_id
  service = "compute.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  project = var.project_id
  service = "bigquery.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project = var.project_id
  service = "storage-api.googleapis.com"

  disable_on_destroy = false
}

# Create GCS buckets
module "gcs_buckets" {
  source = "./modules/gcs_buckets"

  project_id         = var.project_id
  region             = var.region
  environment        = var.environment
  force_destroy      = var.force_destroy_buckets
  enable_versioning  = var.enable_bucket_versioning
  config_file_path   = var.config_file_path
  schema_file_path   = var.schema_file_path
  labels             = var.labels

  depends_on = [
    google_project_service.storage
  ]
}

# Create BigQuery dataset and tables
module "bigquery" {
  source = "./modules/bigquery"

  project_id                 = var.project_id
  region                     = var.bigquery_region
  environment                = var.environment
  dataset_owner_email        = var.dataset_owner_email
  schema_file_path           = var.schema_file_path
  deletion_protection        = var.deletion_protection
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = var.labels

  depends_on = [
    google_project_service.bigquery
  ]
}

# Create IAM service account and permissions
module "iam" {
  source = "./modules/iam"

  project_id        = var.project_id
  environment       = var.environment
  input_bucket_name = module.gcs_buckets.input_bucket_name
  dlq_bucket_name   = module.gcs_buckets.dlq_bucket_name
  temp_bucket_name  = module.gcs_buckets.temp_bucket_name

  depends_on = [
    module.gcs_buckets
  ]
}
