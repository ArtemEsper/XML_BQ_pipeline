# GCS Buckets Module for WoS Dataflow Pipeline
# Creates input, DLQ, and temporary storage buckets

resource "google_storage_bucket" "input_bucket" {
  name          = "${var.project_id}-wos-input-${var.environment}"
  location      = var.region
  force_destroy = var.force_destroy

  uniform_bucket_level_access = true

  versioning {
    enabled = var.enable_versioning
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = merge(
    var.labels,
    {
      purpose     = "wos-xml-input"
      environment = var.environment
    }
  )
}

resource "google_storage_bucket" "dlq_bucket" {
  name          = "${var.project_id}-wos-dlq-${var.environment}"
  location      = var.region
  force_destroy = var.force_destroy

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  labels = merge(
    var.labels,
    {
      purpose     = "wos-dead-letter-queue"
      environment = var.environment
    }
  )
}

resource "google_storage_bucket" "temp_bucket" {
  name          = "${var.project_id}-dataflow-temp-${var.environment}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }

  labels = merge(
    var.labels,
    {
      purpose     = "dataflow-temporary-storage"
      environment = var.environment
    }
  )
}

# Upload config and schema files
resource "google_storage_bucket_object" "wos_config" {
  name   = "config/wos_config.xml"
  bucket = google_storage_bucket.input_bucket.name
  source = var.config_file_path
}

resource "google_storage_bucket_object" "wos_schemas" {
  name   = "config/all_schemas.json"
  bucket = google_storage_bucket.input_bucket.name
  source = var.schema_file_path
}
