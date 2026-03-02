# Variables for main Terraform configuration

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "bigquery_region" {
  description = "BigQuery dataset region (US, EU, etc.)"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "dataset_owner_email" {
  description = "Email of the BigQuery dataset owner"
  type        = string
}

variable "config_file_path" {
  description = "Local path to wos_config.xml"
  type        = string
  default     = "../parser/wos_config.xml"
}

variable "schema_file_path" {
  description = "Local path to all_schemas.json"
  type        = string
  default     = "../config/schemas/all_schemas.json"
}

variable "force_destroy_buckets" {
  description = "Allow deletion of non-empty buckets (use with caution!)"
  type        = bool
  default     = false
}

variable "enable_bucket_versioning" {
  description = "Enable object versioning for input bucket"
  type        = bool
  default     = true
}

variable "deletion_protection" {
  description = "Enable deletion protection for BigQuery tables"
  type        = bool
  default     = true
}

variable "delete_contents_on_destroy" {
  description = "Delete BigQuery dataset contents on destroy"
  type        = bool
  default     = false
}

variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default = {
    managed_by = "terraform"
    project    = "wos-pipeline"
  }
}
