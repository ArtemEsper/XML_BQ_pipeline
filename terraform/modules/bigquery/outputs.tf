# Outputs for BigQuery Module

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.wos_dataset.dataset_id
}

output "dataset_project" {
  description = "Project containing the dataset"
  value       = google_bigquery_dataset.wos_dataset.project
}

output "dataset_location" {
  description = "Dataset location"
  value       = google_bigquery_dataset.wos_dataset.location
}

output "full_dataset_id" {
  description = "Full dataset ID (project:dataset)"
  value       = "${google_bigquery_dataset.wos_dataset.project}:${google_bigquery_dataset.wos_dataset.dataset_id}"
}

output "table_ids" {
  description = "List of all WoS content table IDs"
  value       = [for table in google_bigquery_table.wos_tables : table.table_id]
}

output "table_count" {
  description = "Number of WoS content tables created"
  value       = length(google_bigquery_table.wos_tables)
}

output "record_registry_table_id" {
  description = "Table ID of wos_record_registry"
  value       = google_bigquery_table.wos_record_registry.table_id
}

output "file_registry_table_id" {
  description = "Table ID of wos_file_registry"
  value       = google_bigquery_table.wos_file_registry.table_id
}
