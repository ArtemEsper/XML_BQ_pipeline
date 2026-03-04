# Outputs for main Terraform configuration

# GCS Outputs
output "input_bucket_name" {
  description = "Name of the input XML bucket"
  value       = module.gcs_buckets.input_bucket_name
}

output "dlq_bucket_name" {
  description = "Name of the DLQ bucket"
  value       = module.gcs_buckets.dlq_bucket_name
}

output "temp_bucket_name" {
  description = "Name of the temporary storage bucket"
  value       = module.gcs_buckets.temp_bucket_name
}

output "config_file_gcs_path" {
  description = "GCS path to uploaded config file"
  value       = module.gcs_buckets.config_file_gcs_path
}

output "schema_file_gcs_path" {
  description = "GCS path to uploaded schema file"
  value       = module.gcs_buckets.schema_file_gcs_path
}

# BigQuery Outputs
output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = module.bigquery.dataset_id
}

output "bigquery_full_dataset_id" {
  description = "Full BigQuery dataset ID (project:dataset)"
  value       = module.bigquery.full_dataset_id
}

output "bigquery_table_count" {
  description = "Number of BigQuery tables created"
  value       = module.bigquery.table_count
}

# IAM Outputs
output "dataflow_service_account_email" {
  description = "Email of the Dataflow service account"
  value       = module.iam.service_account_email
}

# Dataflow Command
output "dataflow_run_command" {
  description = "Command to run the Dataflow pipeline"
  value = <<-EOT
    python -m wos_beam_pipeline.main \
      --input_pattern='gs://${module.gcs_buckets.input_bucket_name}/data/*.xml' \
      --config_path='${module.gcs_buckets.config_file_gcs_path}' \
      --schema_path='${module.gcs_buckets.schema_file_gcs_path}' \
      --bq_dataset='${module.bigquery.full_dataset_id}' \
      --dlq_bucket='${module.gcs_buckets.dlq_bucket_name}' \
      --runner=DataflowRunner \
      --project=${var.project_id} \
      --region=${var.region} \
      --temp_location='gs://${module.gcs_buckets.temp_bucket_name}/temp' \
      --service_account_email='${module.iam.service_account_email}' \
      --max_num_workers=50 \
      --machine_type=n2-standard-4 \
      --setup_file="$(pwd)/setup.py"
  EOT
}
