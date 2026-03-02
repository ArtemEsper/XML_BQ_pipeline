# IAM Module for WoS Dataflow Pipeline
# Creates service account and assigns necessary permissions

# Create service account for Dataflow workers
resource "google_service_account" "dataflow_sa" {
  account_id   = "wos-dataflow-sa-${var.environment}"
  display_name = "WoS Dataflow Service Account (${var.environment})"
  description  = "Service account for WoS XML to BigQuery Dataflow pipeline"
  project      = var.project_id
}

# Grant Dataflow Worker role
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Grant BigQuery Data Editor role
resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Grant BigQuery Job User role (for running BigQuery jobs)
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Grant Storage Object Admin role (for reading input, writing DLQ, temp files)
resource "google_project_iam_member" "storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Grant Logging Writer role
resource "google_project_iam_member" "logging_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Grant Monitoring Metric Writer role
resource "google_project_iam_member" "monitoring_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Optional: Grant specific bucket permissions for better security
resource "google_storage_bucket_iam_member" "input_bucket_reader" {
  count  = var.input_bucket_name != "" ? 1 : 0
  bucket = var.input_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "dlq_bucket_writer" {
  count  = var.dlq_bucket_name != "" ? 1 : 0
  bucket = var.dlq_bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "temp_bucket_admin" {
  count  = var.temp_bucket_name != "" ? 1 : 0
  bucket = var.temp_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}
