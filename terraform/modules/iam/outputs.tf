# Outputs for IAM Module

output "service_account_email" {
  description = "Email of the Dataflow service account"
  value       = google_service_account.dataflow_sa.email
}

output "service_account_id" {
  description = "ID of the Dataflow service account"
  value       = google_service_account.dataflow_sa.id
}

output "service_account_name" {
  description = "Name of the Dataflow service account"
  value       = google_service_account.dataflow_sa.name
}
