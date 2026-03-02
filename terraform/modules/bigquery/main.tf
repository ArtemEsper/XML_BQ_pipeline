# BigQuery Module for WoS Dataflow Pipeline
# Creates dataset and tables for WoS data

# Create BigQuery dataset
resource "google_bigquery_dataset" "wos_dataset" {
  dataset_id                 = "wos_${var.environment}"
  friendly_name              = "Web of Science ${title(var.environment)}"
  description                = "Web of Science bibliometric data - ${var.environment} environment"
  location                   = var.region
  default_table_expiration_ms = var.default_table_expiration_ms

  labels = merge(
    var.labels,
    {
      environment = var.environment
      data_source = "web-of-science"
    }
  )

  delete_contents_on_destroy = var.delete_contents_on_destroy

  access {
    role          = "OWNER"
    user_by_email = var.dataset_owner_email
  }
}

# Read schema definitions from JSON file
locals {
  # Load all schemas from the JSON file
  all_schemas = jsondecode(file(var.schema_file_path))

  # Create a map of table name to schema for easier iteration
  tables = { for table_name, schema in local.all_schemas : table_name => schema }
}

# Create BigQuery tables dynamically from schema file
resource "google_bigquery_table" "wos_tables" {
  for_each = local.tables

  dataset_id = google_bigquery_dataset.wos_dataset.dataset_id
  table_id   = each.key

  labels = merge(
    var.labels,
    {
      environment = var.environment
      table_type  = "wos-data"
    }
  )

  # Define schema from JSON
  schema = jsonencode(each.value)

  # Enable clustering for better query performance
  clustering = contains([for field in each.value : field.name], "id") ? ["id"] : null

  deletion_protection = var.deletion_protection
}
