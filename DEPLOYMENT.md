# Deployment Guide

Complete step-by-step guide for deploying the WoS XML to BigQuery Dataflow Pipeline.

## Prerequisites

### Required Tools
- **Google Cloud SDK** (`gcloud`): [Install](https://cloud.google.com/sdk/docs/install)
- **Terraform** (>= 1.0): [Install](https://www.terraform.io/downloads)
- **Python** (>= 3.11): [Install](https://www.python.org/downloads/)
- **Docker**: [Install](https://docs.docker.com/get-docker/) (for Flex Template)
- **Graphviz** (optional): For schema visualization (`pip install graphviz` + system binary)

### GCP Requirements
- Active GCP project with billing enabled
- Project Owner or Editor permissions
- Enabled APIs (Terraform will enable these):
  - Dataflow API
  - BigQuery API
  - Cloud Storage API
  - Compute Engine API

## Step 1: Initial Setup

### 1.1 Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### 1.2 Set Environment Variables

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export ENVIRONMENT="dev"  # or staging, prod
```

## Step 2: Generate BigQuery Schemas

```bash
# From project root
python -m wos_beam_pipeline.utils.schema_generator \
  parser/wos_schema_final.sql \
  config/schemas

# Verify schemas created
ls config/schemas/
```

**Expected output:** 46 `*_schema.json` files + `all_schemas.json`

## Step 3: Deploy Infrastructure with Terraform

### 3.1 Configure Terraform Variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:
```hcl
project_id          = "your-gcp-project-id"
region              = "us-central1"
bigquery_region     = "US"
environment         = "dev"
dataset_owner_email = "your-email@example.com"

# Paths (relative to terraform directory)
config_file_path = "../parser/wos_config.xml"
schema_file_path = "../config/schemas/all_schemas.json"

# Safety settings for dev
force_destroy_buckets      = true   # WARNING: Only for dev!
deletion_protection        = false  # WARNING: Only for dev!
delete_contents_on_destroy = true   # WARNING: Only for dev!
```

**For production, use:**
```hcl
force_destroy_buckets      = false
deletion_protection        = true
delete_contents_on_destroy = false
```

### 3.2 Initialize Terraform

```bash
terraform init
```

### 3.3 Preview Changes

```bash
terraform plan
```

Review the output. Should create:
- 3 GCS buckets
- 1 BigQuery dataset
- 48 BigQuery tables (46 content tables + `wos_record_registry` + `wos_file_registry`)
- 1 Service account
- ~10 IAM role bindings

### 3.4 Apply Configuration

```bash
terraform apply
```

Type `yes` when prompted.

**Wait time:** ~2-5 minutes

### 3.5 Save Outputs

```bash
# Save important values
terraform output -json > outputs.json

# Or view specific outputs
terraform output input_bucket_name
terraform output dataflow_service_account_email
terraform output bigquery_full_dataset_id
```

## Step 4: Upload Sample Data

```bash
# Get bucket name
INPUT_BUCKET=$(terraform output -raw input_bucket_name)

# Upload sample XML file
gsutil cp ../data/WR_2024_20240112153830_CORE_0001.xml \
  gs://${INPUT_BUCKET}/data/

# Verify upload
gsutil ls gs://${INPUT_BUCKET}/data/
```

## Step 5: Test Pipeline Locally (Optional)

### 5.1 Create Virtual Environment

```bash
cd ..  # Back to project root
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

### 5.2 Run with DirectRunner

```bash
# Get configuration from Terraform
cd terraform
CONFIG_PATH=$(terraform output -raw config_file_gcs_path)
SCHEMA_PATH=$(terraform output -raw schema_file_gcs_path)
BQ_DATASET=$(terraform output -raw bigquery_full_dataset_id)
DLQ_BUCKET=$(terraform output -raw dlq_bucket_name)
INPUT_BUCKET=$(terraform output -raw input_bucket_name)

cd ..

# Run pipeline locally
python -m wos_beam_pipeline.main \
  --input_pattern="gs://${INPUT_BUCKET}/data/*.xml" \
  --config_path="${CONFIG_PATH}" \
  --schema_path="${SCHEMA_PATH}" \
  --bq_dataset="${BQ_DATASET}" \
  --dlq_bucket="${DLQ_BUCKET}" \
  --runner=DirectRunner \
  --setup_file="$(pwd)/setup.py"
```

**Expected output:**
- Processing logs showing records being parsed
- BigQuery tables populated
- No (or minimal) DLQ records

**Verification:**
```bash
# Check BigQuery
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`${BQ_DATASET}.wos_summary\`"

# Check DLQ
gsutil ls gs://${DLQ_BUCKET}/failed_records/
```

## Step 6: Run on Dataflow (Production)

### 6.1 Run Directly (without Flex Template)

```bash
# Get service account email
SA_EMAIL=$(cd terraform && terraform output -raw dataflow_service_account_email && cd ..)
TEMP_BUCKET=$(cd terraform && terraform output -raw temp_bucket_name && cd ..)

python -m wos_beam_pipeline.main \
  --input_pattern="gs://${INPUT_BUCKET}/data/*.xml" \
  --config_path="${CONFIG_PATH}" \
  --schema_path="${SCHEMA_PATH}" \
  --bq_dataset="${BQ_DATASET}" \
  --dlq_bucket="${DLQ_BUCKET}" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --temp_location="gs://${TEMP_BUCKET}/temp" \
  --service_account_email="${SA_EMAIL}" \
  --max_num_workers=50 \
  --machine_type=n2-standard-4 \
  --job_name=wos-xml-to-bq-$(date +%Y%m%d-%H%M%S)
```

**Monitor job:**
```bash
# Open Dataflow console
gcloud dataflow jobs list --region=${REGION}

# Or view in web console
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}?project=${PROJECT_ID}"
```

### 6.2 Build and Deploy Flex Template (Advanced)

If you've updated the pipeline code or `wos_config.xml`, you need to push a new Docker image and update the Flex Template spec in GCS.

```bash
# Variables
PROJECT_ID="xml-bq-wos-analytics"
REGION="us-central1"
REPOSITORY="wos-pipeline"
IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/wos-pipeline:latest"

# 1. Authenticate Docker for Artifact Registry
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# 2. Build and push the image
docker build -t "${IMAGE_TAG}" .
docker push "${IMAGE_TAG}"

# 3. Re-build the Flex Template specification in GCS
TEMPLATE_BUCKET=$(cd terraform && terraform output -raw temp_bucket_name && cd ..)
gcloud dataflow flex-template build "gs://${TEMPLATE_BUCKET}/templates/wos_pipeline.json" \
  --image="${IMAGE_TAG}" \
  --sdk-language="PYTHON" \
  --metadata-file="metadata.json"

# 4. Run the updated template
# Optimization: use --parameters "sdk_container_image=${IMAGE_TAG}" to avoid runtime dependency installation
gcloud dataflow flex-template run "wos-xml-to-bq-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location="gs://${TEMPLATE_BUCKET}/templates/wos_pipeline.json" \
  --region="${REGION}" \
  --service-account-email=$(cd terraform && terraform output -raw dataflow_service_account_email && cd ..) \
  --parameters "input_pattern=gs://$(cd terraform && terraform output -raw input_bucket_name && cd ..)/data/*.xml" \
  --parameters "config_path=$(cd terraform && terraform output -raw config_file_gcs_path && cd ..)" \
  --parameters "schema_path=$(cd terraform && terraform output -raw schema_file_gcs_path && cd ..)" \
  --parameters "bq_dataset=$(cd terraform && terraform output -raw bigquery_full_dataset_id && cd ..)" \
  --parameters "dlq_bucket=$(cd terraform && terraform output -raw dlq_bucket_name && cd ..)" \
  --parameters "namespace=http://clarivate.com/schema/wok5.30/public/FullRecord" \
  --parameters "parent_tag=records" \
  --parameters "sdk_container_image=${IMAGE_TAG}"
```

## Step 6b: Running with Idempotent Processing (`--enable_dedup`)

The pipeline supports hash-based idempotent processing that makes it safe to re-run on
the same or updated files without producing duplicate BigQuery rows.

### How It Works

| Level | Mechanism | Benefit |
|-------|-----------|---------|
| **File** | MD5 from GCS object metadata checked against `wos_file_registry` | Entire pipeline skipped if file unchanged |
| **Record** | SHA-256 of raw `<REC>` XML checked against `wos_record_registry` | Only new/changed records are parsed and written |
| **Post-pipeline** | BQ `DELETE` removes old rows for changed records | BigQuery always has exactly one version per UID |

### Prerequisites

**Registry tables must exist.** They are created by Terraform:

```bash
cd terraform
terraform plan   # should show wos_record_registry and wos_file_registry in plan
terraform apply
```

After apply, verify the registry tables exist:

```bash
bq ls ${BQ_DATASET}
# Should include: wos_record_registry, wos_file_registry
```

### 6b.1 First Run (populates the registry)

On the first run with `--enable_dedup`, the record registry is empty so every record
is treated as NEW and written to BigQuery normally. The file is registered in
`wos_file_registry` after the job completes.

**DirectRunner:**
```bash
python -m wos_beam_pipeline.main \
  --input_pattern="gs://${INPUT_BUCKET}/data/*.xml" \
  --config_path="${CONFIG_PATH}" \
  --schema_path="${SCHEMA_PATH}" \
  --bq_dataset="${BQ_DATASET}" \
  --dlq_bucket="${DLQ_BUCKET}" \
  --runner=DirectRunner \
  --enable_dedup
```

**DataflowRunner:**
```bash
python -m wos_beam_pipeline.main \
  --input_pattern="gs://${INPUT_BUCKET}/data/*.xml" \
  --config_path="${CONFIG_PATH}" \
  --schema_path="${SCHEMA_PATH}" \
  --bq_dataset="${BQ_DATASET}" \
  --dlq_bucket="${DLQ_BUCKET}" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --temp_location="gs://${TEMP_BUCKET}/temp" \
  --service_account_email="${SA_EMAIL}" \
  --max_num_workers=50 \
  --machine_type=n2-standard-4 \
  --setup_file=./setup.py \
  --job_name=wos-dedup-$(date +%Y%m%d-%H%M%S) \
  --enable_dedup
```

# Variables (adjust to your project)
PROJECT_ID="xml-bq-wos-analytics"
REGION="us-central1"
REPOSITORY="wos-pipeline"
IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/wos-pipeline:latest"
TEMPLATE_BUCKET="${PROJECT_ID}-dataflow-temp-dev"

# To update the template after code or config changes:
# 1. Authenticate Docker: gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
# 2. Build and push image: docker build -t "${IMAGE_TAG}" . && docker push "${IMAGE_TAG}"
# 3. Update template spec: gcloud dataflow flex-template build "gs://${TEMPLATE_BUCKET}/templates/wos_pipeline.json" --image="${IMAGE_TAG}" --sdk-language="PYTHON" --metadata-file="metadata.json"

# Run the job:
gcloud dataflow flex-template run "wos-xml-to-bq-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location="gs://${TEMPLATE_BUCKET}/templates/wos_pipeline.json" \
  --region="${REGION}" \
  --service-account-email="wos-dataflow-sa-dev@${PROJECT_ID}.iam.gserviceaccount.com" \
  --max-workers=50 \
  --worker-machine-type=n2-standard-4 \
  --parameters "input_pattern=gs://${PROJECT_ID}-wos-input-dev/data/*.xml" \
  --parameters "config_path=gs://${PROJECT_ID}-wos-input-dev/config/wos_config.xml" \
  --parameters "schema_path=gs://${PROJECT_ID}-wos-input-dev/config/all_schemas.json" \
  --parameters "bq_dataset=${PROJECT_ID}:wos_dev" \
  --parameters "dlq_bucket=${PROJECT_ID}-wos-dlq-dev" \
  --parameters "namespace=http://clarivate.com/schema/wok5.30/public/FullRecord" \
  --parameters "parent_tag=records" \
  --parameters "bq_write_disposition=WRITE_APPEND" \
  --parameters "enable_dedup=true"
```

### 6b.2 Subsequent Runs (idempotent behaviour)

| Scenario | Pipeline Behaviour |
|----------|--------------------|
| **Same file, same content** | File MD5 matches registry → entire job exits immediately |
| **Same file, some records changed** | File MD5 differs → record-level comparison; only new/changed records written; old rows for changed UIDs deleted |
| **New file** | No registry entry → all records treated as NEW |

### 6b.3 Verifying Idempotent Behaviour

After a dedup run, check the registries:

```sql
-- How many records are registered?
SELECT COUNT(*) AS total_records
FROM `{PROJECT}.{DATASET}.wos_record_registry`;

-- Most recently registered records
SELECT uid, source_file, ingested_at
FROM `{PROJECT}.{DATASET}.wos_record_registry`
ORDER BY ingested_at DESC
LIMIT 20;

-- Files processed
SELECT file_path, file_md5, processed_at, record_count
FROM `{PROJECT}.{DATASET}.wos_file_registry`
ORDER BY processed_at DESC;
```

Verify no duplicate UIDs in the main table:

```sql
-- Should return 0 if idempotence is working correctly
SELECT uid, COUNT(*) AS cnt
FROM `{PROJECT}.{DATASET}.wos_summary`
GROUP BY uid
HAVING cnt > 1
LIMIT 10;
```

### 6b.4 Schema Requirements

The `--enable_dedup` flag injects two NULLABLE fields into all tables at runtime:

| Field | Type | Tables |
|-------|------|--------|
| `ingestion_ts` | TIMESTAMP | All 46 content tables |
| `record_hash` | STRING | `wos_summary` only |

On the first run, `WriteToBigQuery` automatically adds these columns to existing tables
via `schemaUpdateOptions: ALLOW_FIELD_ADDITION`. No manual `ALTER TABLE` is needed.

---

### 6.3 Updating the BigQuery Schema

When you add new columns to `wos_schema_final.sql` or change the structure, follow this flow to update the existing BigQuery tables:

1.  **Re-generate JSON schemas:**
    ```bash
    python -m wos_beam_pipeline.utils.schema_generator \
      parser/wos_schema_final.sql \
      config/schemas
    ```

2.  **Apply changes with Terraform:**
    Navigate to the `terraform` directory and run apply. Terraform will detect the schema changes in `all_schemas.json` and update the BigQuery tables in-place.
    ```bash
    cd terraform
    terraform apply
    ```
    *Note: BigQuery supports adding new nullable columns to existing tables without data loss.*

3.  **Verify the update:**
    Check if the new column exists in BigQuery:
    ```bash
    bq query --use_legacy_sql=false \
      "SELECT column_name, data_type FROM \`${PROJECT_ID}.${ENVIRONMENT}.INFORMATION_SCHEMA.COLUMNS\` WHERE table_name = 'wos_summary' AND column_name = 'r_id_disclaimer'"
    ```

## Step 7: Monitoring and Validation

### 7.1 Check Pipeline Status

```bash
# List running jobs
gcloud dataflow jobs list --region=${REGION} --status=active

# Get job details
JOB_ID="<job-id-from-above>"
gcloud dataflow jobs describe ${JOB_ID} --region=${REGION}
```

### 7.2 View Logs

```bash
# Stream logs
gcloud dataflow logs --region=${REGION} ${JOB_ID}

# Or view in Cloud Logging
https://console.cloud.google.com/logs/query?project=${PROJECT_ID}
```

### 7.3 Validate Data in BigQuery

```bash
# Record count
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as record_count FROM \`${BQ_DATASET}.wos_summary\`"

# Sample records
bq query --use_legacy_sql=false \
  "SELECT id, pubyear, vol, issue FROM \`${BQ_DATASET}.wos_summary\` LIMIT 10"

# Table statistics
bq ls --format=pretty ${BQ_DATASET}
```

### 7.4 Check DLQ

```bash
# List DLQ files
gsutil ls -r gs://${DLQ_BUCKET}/failed_records/

# Download and inspect a DLQ file
gsutil cat gs://${DLQ_BUCKET}/failed_records/latest.jsonl | jq .
```

## Step 8: Cleanup (Optional)

### 8.1 Delete Pipeline Run

```bash
# Cancel running job
gcloud dataflow jobs cancel ${JOB_ID} --region=${REGION}
```

### 8.2 Delete Infrastructure

```bash
cd terraform

# Preview what will be deleted
terraform plan -destroy

# Delete all resources
terraform destroy
```

**WARNING:** This will delete:
- All GCS buckets and their contents
- BigQuery dataset and all tables
- Service account

## Production Deployment Checklist

- [ ] Change environment to `prod` in `terraform.tfvars`
- [ ] Set safety flags:
  - [ ] `force_destroy_buckets = false`
  - [ ] `deletion_protection = true`
  - [ ] `delete_contents_on_destroy = false`
- [ ] Enable Terraform state backend (GCS)
- [ ] Set up Cloud Monitoring alerts
- [ ] Configure log retention policies
- [ ] Set up backup/disaster recovery
- [ ] Use preemptible workers for cost savings
- [ ] Test DLQ recovery process
- [ ] Document runbook for on-call

## Multi-Environment Setup

### Directory Structure
```
terraform/
├── environments/
│   ├── dev.tfvars
│   ├── staging.tfvars
│   └── prod.tfvars
```

### Deploy to Specific Environment
```bash
# Dev
terraform apply -var-file=environments/dev.tfvars

# Staging
terraform apply -var-file=environments/staging.tfvars

# Prod
terraform apply -var-file=environments/prod.tfvars
```

## Troubleshooting

### Issue: Terraform State Lock
**Error:** `Error locking state: Error acquiring the state lock`
**Solution:**
```bash
terraform force-unlock <LOCK_ID>
```

### Issue: Permission Denied
**Error:** `Permission denied on resource project`
**Solution:**
```bash
# Grant yourself necessary roles
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="user:your-email@example.com" \
  --role="roles/editor"
```

### Issue: API Not Enabled
**Error:** `API dataflow.googleapis.com is not enabled`
**Solution:**
```bash
gcloud services enable dataflow.googleapis.com \
  compute.googleapis.com \
  bigquery.googleapis.com \
  storage-api.googleapis.com
```

### Issue: Quota Exceeded
**Error:** `Quota exceeded for quota metric`
**Solution:**
- Request quota increase in GCP Console
- Or reduce `max_num_workers`

### Issue: Registry table not found (`--enable_dedup`)
**Error:** `404 Not found: Table ... wos_record_registry`
**Solution:**
```bash
# Deploy Terraform to create registry tables
cd terraform && terraform apply
```

### Issue: Pipeline exits immediately (`--enable_dedup`)
**Cause:** File MD5 matches `wos_file_registry` — file was already processed.
**Expected behaviour** — this is correct idempotent operation.
To force a re-run, delete the registry entry:
```sql
DELETE FROM `{PROJECT}.{DATASET}.wos_file_registry`
WHERE file_path = 'gs://your-bucket/data/your-file.xml';
```

### Issue: `ValueError: --project must be provided when --enable_dedup is set`
**Solution:** Pass `--project` explicitly:
```bash
python -m wos_beam_pipeline.main ... --project=${PROJECT_ID} --enable_dedup
```
Or include it in `--bq_dataset` as `project:dataset`.

## Next Steps

1. Set up CI/CD pipeline (Cloud Build, GitHub Actions)
2. Configure monitoring dashboards
3. Set up alerting policies
4. Create data validation queries
5. Document operational runbook
6. Schedule regular runs with Cloud Scheduler

## Step 7: Schema Visualization (Optional)

You can generate a visual diagram of the 46+ normalized tables and their relationships.

### 7.1 Install Requirements
```bash
pip install graphviz
# Also ensure Graphviz system binary is installed (e.g., brew install graphviz)
```

### 7.2 Run Visualization Script
```bash
# From project root
python terraform/scripts/visualize_schema.py
```

### 7.3 Render Diagram
```bash
# To PNG
dot -Tpng wos_schema.dot -o wos_schema.png

# To SVG
dot -Tsvg wos_schema.dot -o wos_schema.svg
```

*Note: If `dot` command is not available (e.g., on macOS 13), use the Python script above to generate the `.dot` file and paste its content into [Graphviz Online](https://dreampuf.github.io/GraphvizOnline/).*

## Support

For deployment issues:
1. Check logs: `gcloud dataflow logs`
2. Review Terraform state: `terraform show`
3. Verify GCP quotas: Console → IAM & Admin → Quotas
4. Check service account permissions: Console → IAM & Admin → IAM
