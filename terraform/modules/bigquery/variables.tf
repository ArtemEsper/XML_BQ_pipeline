# Variables for BigQuery Module

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for BigQuery dataset"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "dataset_owner_email" {
  description = "Email of the dataset owner"
  type        = string
}

variable "schema_file_path" {
  description = "Path to all_schemas.json file"
  type        = string
  default     = "../../config/schemas/all_schemas.json"
}

variable "default_table_expiration_ms" {
  description = "Default table expiration in milliseconds (null for no expiration)"
  type        = number
  default     = null
}

variable "deletion_protection" {
  description = "Enable deletion protection for tables"
  type        = bool
  default     = true
}

variable "delete_contents_on_destroy" {
  description = "Delete dataset contents on destroy"
  type        = bool
  default     = false
}

variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default     = {}
}
