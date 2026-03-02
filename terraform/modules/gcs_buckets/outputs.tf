# Outputs for GCS Buckets Module

output "input_bucket_name" {
  description = "Name of the input XML bucket"
  value       = google_storage_bucket.input_bucket.name
}

output "input_bucket_url" {
  description = "GCS URL of the input bucket"
  value       = google_storage_bucket.input_bucket.url
}

output "dlq_bucket_name" {
  description = "Name of the DLQ bucket"
  value       = google_storage_bucket.dlq_bucket.name
}

output "dlq_bucket_url" {
  description = "GCS URL of the DLQ bucket"
  value       = google_storage_bucket.dlq_bucket.url
}

output "temp_bucket_name" {
  description = "Name of the temporary storage bucket"
  value       = google_storage_bucket.temp_bucket.name
}

output "temp_bucket_url" {
  description = "GCS URL of the temp bucket"
  value       = google_storage_bucket.temp_bucket.url
}

output "config_file_gcs_path" {
  description = "GCS path to config file"
  value       = "gs://${google_storage_bucket.input_bucket.name}/${google_storage_bucket_object.wos_config.name}"
}

output "schema_file_gcs_path" {
  description = "GCS path to schema file"
  value       = "gs://${google_storage_bucket.input_bucket.name}/${google_storage_bucket_object.wos_schemas.name}"
}
