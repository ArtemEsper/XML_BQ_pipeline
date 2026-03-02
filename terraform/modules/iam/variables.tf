# Variables for IAM Module

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "input_bucket_name" {
  description = "Name of the input bucket (for specific permissions)"
  type        = string
  default     = ""
}

variable "dlq_bucket_name" {
  description = "Name of the DLQ bucket (for specific permissions)"
  type        = string
  default     = ""
}

variable "temp_bucket_name" {
  description = "Name of the temp bucket (for specific permissions)"
  type        = string
  default     = ""
}
