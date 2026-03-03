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

# ---------------------------------------------------------------------------
# Registry tables for hash-based idempotent processing
# ---------------------------------------------------------------------------

# wos_record_registry — stores per-record SHA-256 hashes for deduplication
resource "google_bigquery_table" "wos_record_registry" {
  dataset_id = google_bigquery_dataset.wos_dataset.dataset_id
  table_id   = "wos_record_registry"

  description = "Per-record SHA-256 hash registry for idempotent pipeline processing"

  labels = merge(
    var.labels,
    {
      environment = var.environment
      table_type  = "registry"
    }
  )

  schema = jsonencode([
    {
      name        = "uid"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Web of Science unique identifier"
    },
    {
      name        = "record_hash"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "SHA-256 hex digest of the raw <REC> XML string"
    },
    {
      name        = "source_file"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "GCS path of the source XML file"
    },
    {
      name        = "ingested_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "UTC timestamp of the pipeline run that last wrote this record"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  clustering = ["uid"]

  deletion_protection = var.deletion_protection
}

# wos_file_registry — tracks processed files by MD5 to skip unchanged re-runs
resource "google_bigquery_table" "wos_file_registry" {
  dataset_id = google_bigquery_dataset.wos_dataset.dataset_id
  table_id   = "wos_file_registry"

  description = "File-level MD5 registry to skip re-processing unchanged input files"

  labels = merge(
    var.labels,
    {
      environment = var.environment
      table_type  = "registry"
    }
  )

  schema = jsonencode([
    {
      name        = "file_path"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "GCS path of the processed XML file"
    },
    {
      name        = "file_md5"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Base64-encoded MD5 hash from GCS object metadata"
    },
    {
      name        = "processed_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "UTC timestamp when the file was last successfully processed"
    },
    {
      name        = "record_count"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Number of records processed from this file"
    }
  ])

  time_partitioning {
    type  = "DAY"
    field = "processed_at"
  }

  clustering = ["file_path"]

  deletion_protection = var.deletion_protection
}
