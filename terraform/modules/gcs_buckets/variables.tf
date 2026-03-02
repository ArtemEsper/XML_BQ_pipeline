# Variables for GCS Buckets Module

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for buckets"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "force_destroy" {
  description = "Allow deletion of non-empty buckets"
  type        = bool
  default     = false
}

variable "enable_versioning" {
  description = "Enable object versioning for input bucket"
  type        = bool
  default     = true
}

variable "labels" {
  description = "Labels to apply to all resources"
  type        = map(string)
  default     = {}
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
