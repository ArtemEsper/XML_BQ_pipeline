# Deployment Guide

Complete step-by-step guide for deploying the WoS XML to BigQuery Dataflow Pipeline.

## Prerequisites

### Required Tools
- **Google Cloud SDK** (`gcloud`): [Install](https://cloud.google.com/sdk/docs/install)
- **Terraform** (>= 1.0): [Install](https://www.terraform.io/downloads)
- **Python** (>= 3.11): [Install](https://www.python.org/downloads/)
- **Docker**: [Install](https://docs.docker.com/get-docker/) (for Flex Template)

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
python src/wos_beam_pipeline/utils/schema_generator.py \
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
- 46 BigQuery tables
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
python src/wos_beam_pipeline/main.py \
  --input_pattern="gs://${INPUT_BUCKET}/data/*.xml" \
  --config_path="${CONFIG_PATH}" \
  --schema_path="${SCHEMA_PATH}" \
  --bq_dataset="${BQ_DATASET}" \
  --dlq_bucket="${DLQ_BUCKET}" \
  --runner=DirectRunner
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

python src/wos_beam_pipeline/main.py \
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

```bash
# Build Docker image
docker build -t gcr.io/${PROJECT_ID}/wos-pipeline:latest .

# Push to Container Registry
docker push gcr.io/${PROJECT_ID}/wos-pipeline:latest

# Create template bucket if needed
TEMPLATE_BUCKET="${PROJECT_ID}-dataflow-templates"
gsutil mb -l ${REGION} gs://${TEMPLATE_BUCKET} || true

# Build Flex Template
gcloud dataflow flex-template build \
  gs://${TEMPLATE_BUCKET}/wos-pipeline.json \
  --image=gcr.io/${PROJECT_ID}/wos-pipeline:latest \
  --sdk-language=PYTHON \
  --metadata-file=metadata.json

# Run Flex Template
gcloud dataflow flex-template run wos-pipeline-${ENVIRONMENT} \
  --template-file-gcs-location=gs://${TEMPLATE_BUCKET}/wos-pipeline.json \
  --region=${REGION} \
  --service-account-email=${SA_EMAIL} \
  --parameters input_pattern="gs://${INPUT_BUCKET}/data/*.xml" \
  --parameters config_path="${CONFIG_PATH}" \
  --parameters schema_path="${SCHEMA_PATH}" \
  --parameters bq_dataset="${BQ_DATASET}" \
  --parameters dlq_bucket="${DLQ_BUCKET}"
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

## Next Steps

1. Set up CI/CD pipeline (Cloud Build, GitHub Actions)
2. Configure monitoring dashboards
3. Set up alerting policies
4. Create data validation queries
5. Document operational runbook
6. Schedule regular runs with Cloud Scheduler

## Support

For deployment issues:
1. Check logs: `gcloud dataflow logs`
2. Review Terraform state: `terraform show`
3. Verify GCP quotas: Console → IAM & Admin → Quotas
4. Check service account permissions: Console → IAM & Admin → IAM
