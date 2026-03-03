# WoS XML → BigQuery Dataflow Pipeline

[![Tests](https://github.com/ArtemEsper/XML_BQ_pipeline/actions/workflows/test.yml/badge.svg)](https://github.com/ArtemEsper/XML_BQ_pipeline/actions/workflows/test.yml)
[![Build & Deploy](https://github.com/ArtemEsper/XML_BQ_pipeline/actions/workflows/deploy.yml/badge.svg)](https://github.com/ArtemEsper/XML_BQ_pipeline/actions/workflows/deploy.yml)

A production-ready Google Cloud Dataflow pipeline that processes Web of Science (WoS) XML files directly from GCS into 46 normalized BigQuery tables. Replaces a legacy multi-stage ETL (XML → SQL → CSV → BigQuery) with a single, scalable, config-driven pipeline.

## Overview

- Reads XML files (778 MB+, 22 K+ records per file) directly from Google Cloud Storage
- Parses hierarchical XML into 46 normalized BigQuery tables using a config-driven mapping
- **Hash-based idempotent processing** — re-running on the same file produces zero duplicate rows; only new or changed records are written
- Failed records go to a Dead Letter Queue (DLQ) in GCS as enriched JSON lines
- Fully automated CI/CD: every push to `main` that touches pipeline code rebuilds the Docker image, pushes to Artifact Registry, and updates the Flex Template spec

## Architecture

```
Developer pushes to main
        │
        ▼
┌─────────────────────────────────────────────────────┐
│               GitHub Actions CI/CD                  │
│  test.yml         → unit tests (pytest)             │
│  deploy.yml       → docker build + push → AR        │
│                   → flex-template build → GCS       │
└─────────────────────────────────────────────────────┘
        │
        ▼ template spec updated in GCS
        │
gcloud dataflow flex-template run  (manual trigger)
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│                     Dataflow Pipeline                        │
│                                                              │
│  MatchFiles → SplitXMLRecords (uid, xml, sha256_hash)        │
│                        │                                     │
│               [--enable_dedup]                               │
│                        │                                     │
│               FilterUnchangedRecords ←── wos_record_registry │
│               ├── new      ──┐                               │
│               ├── changed ──┤→ Flatten → ParseXMLRecord      │
│               └── unchanged  (discarded)                     │
│                                      │                       │
│                                      ▼                       │
│                             WriteToBigQuery                  │
│                             (46 tables + ingestion_ts)       │
│                                                              │
│  [post-pipeline]                                             │
│  DELETE stale rows for changed UIDs                          │
│  UPDATE wos_record_registry + wos_file_registry              │
└──────────────────────────────────────────────────────────────┘
        │                   │
        ▼                   ▼
gs://…-wos-dlq/     BigQuery dataset wos_<env>
failed_records/     46 content tables
                    wos_record_registry
                    wos_file_registry
```

## Project Structure

```
XML_BQ_pipeline/
├── .github/
│   └── workflows/
│       ├── test.yml          # Unit tests on every push / PR
│       └── deploy.yml        # Docker build + push + Flex Template rebuild
├── scripts/
│   └── setup_wif.sh          # One-time GCP Workload Identity Federation setup
├── src/
│   └── wos_beam_pipeline/
│       ├── main.py           # Pipeline entry point (argparse + beam.Pipeline)
│       ├── models/           # Table, Column, TableList data classes
│       ├── transforms/
│       │   ├── xml_splitter.py   # Splits XML → (uid, xml_string, sha256_hash) tuples
│       │   ├── xml_parser.py     # Parses records into 46 table rows
│       │   ├── dedup.py          # FilterUnchangedRecords DoFn (hash-based dedup)
│       │   ├── schema_validator.py
│       │   └── dlq_handler.py
│       └── utils/
│           ├── config_parser.py
│           ├── schema_generator.py
│           └── registry.py       # BQ registry read/write/cleanup helpers
├── terraform/                # GCP infrastructure (buckets, BQ dataset, IAM)
├── config/schemas/           # 46 auto-generated BigQuery JSON schemas
├── parser/
│   ├── wos_config.xml        # XML → table mapping (the "schema" for the parser)
│   └── wos_schema_final.sql  # PostgreSQL source schema (used to generate BQ schemas)
├── tests/unit/               # 42 unit tests (no GCP required)
├── Dockerfile                # Dataflow Flex Template image
├── launcher.py               # Flex Template entry point (avoids relative-import issues)
├── metadata.json             # Flex Template parameter definitions (11 params)
├── requirements.txt
└── setup.py
```

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | Local development and DirectRunner |
| `gcloud` CLI | latest | Deploy infrastructure and launch jobs |
| Terraform | 1.0+ | Provision GCP resources |
| Docker | latest | Build the Flex Template image locally |
| `gh` CLI | latest | Manage GitHub secrets (CI/CD setup only) |

You also need a GCP project with billing enabled and `gcloud auth application-default login` completed.

---

## Initial Setup (one-time)

### 1. Clone and install

```bash
git clone https://github.com/ArtemEsper/XML_BQ_pipeline.git
cd XML_BQ_pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
```

### 2. Generate BigQuery schemas

The BQ schemas are derived from the PostgreSQL source schema:

```bash
python src/wos_beam_pipeline/utils/schema_generator.py \
  parser/wos_schema_final.sql \
  config/schemas
```

This creates `config/schemas/all_schemas.json` (consumed by Terraform and the pipeline).

### 3. Deploy GCP infrastructure with Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set project_id, environment, dataset_owner_email
terraform init
terraform plan
terraform apply
```

Terraform provisions:
- **GCS buckets**: `<project>-wos-input-<env>`, `<project>-wos-dlq-<env>`, `<project>-dataflow-temp-<env>`
- **BigQuery dataset**: `wos_<env>` with 46 tables
- **Service account**: `wos-dataflow-sa-<env>@...` with appropriate IAM roles
- Uploads `wos_config.xml` and `all_schemas.json` to the input bucket

After `terraform apply`, retrieve the exact run commands:

```bash
terraform output dataflow_run_command
terraform output -json  # all bucket/dataset names
```

### 4. Upload XML data

```bash
gsutil -m cp data/*.xml gs://<project>-wos-input-<env>/data/
```

---

## Running the Pipeline

There are three ways to run the pipeline, suited to different contexts.

### Method 1 — DirectRunner (local development)

Runs on your machine using all available cores. Good for smoke-testing logic on a small XML sample; not suitable for large files.

```bash
python -m wos_beam_pipeline.main \
  --input_pattern='gs://<project>-wos-input-dev/data/sample.xml' \
  --config_path='gs://<project>-wos-input-dev/config/wos_config.xml' \
  --schema_path='gs://<project>-wos-input-dev/config/all_schemas.json' \
  --bq_dataset='<project>:wos_dev' \
  --dlq_bucket='<project>-wos-dlq-dev' \
  --namespace='http://clarivate.com/schema/wok5.30/public/FullRecord' \
  --parent_tag='records' \
  --runner=DirectRunner
```

### Method 2 — DataflowRunner (direct Python invocation)

Submits directly to Dataflow without the Flex Template layer. Useful for one-off runs during development when you haven't rebuilt the template yet.

```bash
python -m wos_beam_pipeline.main \
  --input_pattern='gs://<project>-wos-input-dev/data/*.xml' \
  --config_path='gs://<project>-wos-input-dev/config/wos_config.xml' \
  --schema_path='gs://<project>-wos-input-dev/config/all_schemas.json' \
  --bq_dataset='<project>:wos_dev' \
  --dlq_bucket='<project>-wos-dlq-dev' \
  --namespace='http://clarivate.com/schema/wok5.30/public/FullRecord' \
  --parent_tag='records' \
  --runner=DataflowRunner \
  --project=<project> \
  --region=us-central1 \
  --temp_location='gs://<project>-dataflow-temp-dev/temp' \
  --service_account_email='wos-dataflow-sa-dev@<project>.iam.gserviceaccount.com' \
  --setup_file="$(pwd)/setup.py" \
  --max_num_workers=50 \
  --machine_type=n2-standard-4 \
  --job_name="wos-xml-to-bq-$(date +%Y%m%d-%H%M%S)"
```

> **Note**: `--setup_file` is required when using DataflowRunner so Dataflow workers can install the `wos_beam_pipeline` package. The Flex Template method handles this automatically via the Docker image.

### Method 3 — Flex Template via `gcloud` (production, recommended)

This is the **production method**. The Docker image containing the pipeline code is pre-built by CI/CD and stored in Artifact Registry. Launching a job is a single `gcloud` call — no Python or local environment needed.

```bash
gcloud dataflow flex-template run "wos-xml-to-bq-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location='gs://<project>-dataflow-temp-dev/templates/wos_pipeline.json' \
  --region=us-central1 \
  --service-account-email='wos-dataflow-sa-dev@<project>.iam.gserviceaccount.com' \
  --max-workers=50 \
  --worker-machine-type=n2-standard-4 \
  --parameters 'input_pattern=gs://<project>-wos-input-dev/data/*.xml' \
  --parameters 'config_path=gs://<project>-wos-input-dev/config/wos_config.xml' \
  --parameters 'schema_path=gs://<project>-wos-input-dev/config/all_schemas.json' \
  --parameters 'bq_dataset=<project>:wos_dev' \
  --parameters 'dlq_bucket=<project>-wos-dlq-dev' \
  --parameters 'namespace=http://clarivate.com/schema/wok5.30/public/FullRecord' \
  --parameters 'parent_tag=records' \
  --parameters 'bq_write_disposition=WRITE_APPEND'
```

> **Shell quoting**: In `zsh`, always single-quote `--parameters` values that contain `*`, `://`, or `:`. Unquoted `*.xml` will be expanded by the shell before being passed to `gcloud`.

**Check job status:**

```bash
# List recent jobs
gcloud dataflow jobs list --region=us-central1 --filter="name:wos-xml-to-bq" --limit=5

# Monitor a specific job
gcloud dataflow jobs describe <JOB_ID> --region=us-central1 --format='value(currentState)'

# Stream logs
gcloud logging read \
  'resource.type="dataflow_step" AND resource.labels.job_name~"wos-xml-to-bq"' \
  --freshness=1h --format='table(timestamp, textPayload)'
```

#### Pipeline Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `input_pattern` | yes | — | GCS glob for input XML files, e.g. `gs://bucket/data/*.xml` |
| `config_path` | yes | — | GCS path to `wos_config.xml` |
| `schema_path` | yes | — | GCS path to `all_schemas.json` |
| `bq_dataset` | yes | — | BigQuery destination: `project:dataset` or `dataset` |
| `dlq_bucket` | yes | — | GCS bucket name for failed records (no `gs://` prefix) |
| `namespace` | no | `""` | XML namespace URI, e.g. `http://clarivate.com/schema/wok5.30/public/FullRecord` |
| `parent_tag` | no | `records` | XML tag wrapping the record collection (used for config path lookups) |
| `record_tag` | no | `REC` | XML tag that identifies a single record |
| `id_tag` | no | `UID` | XML tag containing the unique record identifier |
| `file_number` | no | `-1` | Integer tracking field injected into `wos_summary` rows |
| `bq_write_disposition` | no | `WRITE_APPEND` | `WRITE_APPEND`, `WRITE_TRUNCATE`, or `WRITE_EMPTY` |
| `enable_dedup` | no | `false` | Enable hash-based idempotent processing (see section below) |

---

## Idempotent Processing (`--enable_dedup`)

### Problem

Without deduplication, re-running the pipeline on the same file appends duplicate rows to every BigQuery table. When WoS delivers updated files (historical corrections mixed with new records), there is no way to efficiently skip unchanged records or update changed ones cleanly.

### Solution

The `--enable_dedup` flag activates a two-level idempotency mechanism:

| Level | Mechanism | Effect |
|-------|-----------|--------|
| **File** | MD5 hash from GCS object metadata compared against `wos_file_registry` | Entire pipeline skipped if file unchanged since last run |
| **Record** | SHA-256 of raw `<REC>` XML string compared against `wos_record_registry` | Only new/changed records are parsed and written |

### Data Flow (dedup enabled)

```
SplitXMLRecords
  yields (uid, xml_string, sha256_hash)
        │
        ▼
FilterUnchangedRecords  ← side input: {uid → hash} from wos_record_registry
  │
  ├── OUTPUT_TAG_NEW       (uid not in registry)
  ├── OUTPUT_TAG_CHANGED   (uid in registry, hash differs)  → also written to GCS temp
  └── OUTPUT_TAG_UNCHANGED (uid in registry, hash matches)  → DISCARDED
        │
        ▼ Flatten([new, changed])
ParseXMLRecord
  ├── injects ingestion_ts into every row
  └── injects record_hash into wos_summary row
        │
        ▼
WriteToBigQuery (WRITE_APPEND — only new/changed rows)
        │
        ▼ [post-pipeline, outside Beam graph]
DELETE FROM each table WHERE id IN changed_uids AND ingestion_ts < run_ingestion_ts
UPDATE wos_record_registry  (new hashes inserted, changed hashes updated)
UPDATE wos_file_registry    (file MD5 + processed_at recorded)
```

### Registry Tables

Two registry tables live in the same BigQuery dataset as the 46 content tables:

**`wos_record_registry`** — one row per WoS record, updated every run:

| Column | Type | Description |
|--------|------|-------------|
| `uid` | STRING REQUIRED | WoS unique identifier |
| `record_hash` | STRING REQUIRED | SHA-256 hex digest of the raw `<REC>` XML |
| `source_file` | STRING NULLABLE | GCS path of the source XML file |
| `ingested_at` | TIMESTAMP REQUIRED | UTC timestamp of the last pipeline run that touched this record |

Partitioned by `ingested_at` (DAY), clustered on `uid`.

**`wos_file_registry`** — one row per processed file:

| Column | Type | Description |
|--------|------|-------------|
| `file_path` | STRING REQUIRED | GCS path of the input XML file |
| `file_md5` | STRING REQUIRED | Base64-encoded MD5 from GCS object metadata |
| `processed_at` | TIMESTAMP REQUIRED | UTC timestamp of last successful processing |
| `record_count` | INTEGER REQUIRED | Number of records processed |

Partitioned by `processed_at` (DAY), clustered on `file_path`.

### Schema Changes to Content Tables

Two NULLABLE columns are added to all 46 content tables at runtime. They are backward-compatible — existing rows simply have `NULL` in these columns.

| Column | Type | Added to |
|--------|------|----------|
| `ingestion_ts` | TIMESTAMP NULLABLE | All 46 tables |
| `record_hash` | STRING NULLABLE | `wos_summary` only |

`ingestion_ts` is the key used by the post-pipeline `DELETE` to remove stale rows for changed records without touching unchanged rows from other pipeline runs.

### Running with Dedup

**First run** (empty registry — all records treated as NEW):

```bash
python -m wos_beam_pipeline.main \
  --input_pattern='gs://<project>-wos-input-dev/data/*.xml' \
  ... [standard args] ... \
  --runner=DataflowRunner \
  --enable_dedup
```

**Subsequent run, same file** — file MD5 unchanged → pipeline exits immediately:

```
INFO - File gs://…/data/WR_2024_*.xml already processed with same MD5 — skipping pipeline run.
```

**Subsequent run, updated file** — only changed/new records are processed:

```
INFO - Record registry loaded: 22,659 entries
INFO - Post-pipeline cleanup: deleting old rows for 143 changed records
INFO - Cleanup completed successfully for all tables
```

**Via Flex Template** (production):

```bash
gcloud dataflow flex-template run "wos-xml-to-bq-dedup-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location='gs://<project>-dataflow-temp-dev/templates/wos_pipeline.json' \
  --region=us-central1 \
  ... [standard params] ... \
  --parameters 'enable_dedup=true'
```

### Verifying Idempotency

After the first dedup run:

```sql
-- Registry should have one entry per record
SELECT COUNT(*) FROM `<project>.wos_dev.wos_record_registry`;
-- Expected: ~22,659

-- File should be registered
SELECT file_path, file_md5, processed_at, record_count
FROM `<project>.wos_dev.wos_file_registry`;

-- No duplicate UIDs in summary
SELECT COUNT(*), COUNT(DISTINCT id) FROM `<project>.wos_dev.wos_summary`;
-- Both counts should be equal

-- Confirm ingestion_ts is populated
SELECT id, ingestion_ts, record_hash
FROM `<project>.wos_dev.wos_summary`
LIMIT 5;
```

After re-running the same file with `--enable_dedup`:

```sql
-- Row counts in content tables should be unchanged
SELECT COUNT(*) FROM `<project>.wos_dev.wos_summary`;

-- No new ingestion_ts values (nothing was written)
SELECT DISTINCT DATE(ingestion_ts) FROM `<project>.wos_dev.wos_summary`;
```

### Dedup Cost and Performance

| Overhead | Detail |
|----------|--------|
| Registry load | One BQ query at startup; ~1–2 s for 22K entries |
| SHA-256 hashing | <1 ms per record; negligible vs XML parse time |
| Post-pipeline cleanup | ~5 s per table × 46 tables ≈ 4 min for a full changed-record pass |
| File-level skip | Sub-second (GCS metadata + 1 BQ query); avoids all Dataflow costs |

For a file with no changes, total overhead is under 5 seconds and Dataflow is never started.

---

## CI/CD Pipeline

Every push to the `main` branch is handled by two GitHub Actions workflows.

### Workflow: `test.yml` — Unit Tests

**Triggers:** every push to `main` and every pull request targeting `main`.

```
push / pull_request → main
        │
        ▼
  ubuntu-latest runner
  Python 3.11
  pip install -r requirements.txt + pip install -e .
  pytest tests/unit/ -v --cov=src/wos_beam_pipeline
        │
        ▼
  Coverage report uploaded as artifact
```

All 42 unit tests cover the config parser, schema generator, table model, XML splitter (including SHA-256 hash output), deduplication routing, and registry functions.

### Workflow: `deploy.yml` — Build & Deploy

**Triggers:** push to `main` **only when** these paths change:

```
src/**  |  Dockerfile  |  launcher.py  |  requirements.txt
setup.py  |  metadata.json  |  .github/workflows/deploy.yml
```

Changes to tests, docs, Terraform, or other non-pipeline files do **not** trigger a rebuild.

```
push to main (pipeline files changed)
        │
        ▼
Job 1: Build & Push Docker Image
  ├─ Authenticate to GCP via Workload Identity Federation
  ├─ docker/login-action → us-central1-docker.pkg.dev
  ├─ docker/build-push-action (--platform linux/amd64)
  │   Tags: :latest  AND  :<git-sha-8>
  └─ Layer cache via GitHub Actions Cache (gha)
        │
        ▼ (on success)
Job 2: Build Flex Template Spec
  ├─ Authenticate to GCP via Workload Identity Federation
  ├─ gcloud dataflow flex-template build
  │   --image=…:latest  --metadata-file=metadata.json
  └─ Writes wos_pipeline.json to GCS
```

The Docker image is always built for `linux/amd64` (required for Dataflow workers), regardless of the developer's local CPU architecture.

### Authentication: Workload Identity Federation

The workflows authenticate to GCP **without any long-lived service account keys**. Instead, GitHub's OIDC token is exchanged for a short-lived GCP access token using Workload Identity Federation.

```
GitHub Actions runner
    │  requests OIDC token (id-token: write permission)
    ▼
GitHub OIDC Provider (token.actions.githubusercontent.com)
    │  JWT token scoped to ArtemEsper/XML_BQ_pipeline
    ▼
GCP Workload Identity Pool (github-actions-pool)
    │  validates token, checks repository attribute
    ▼
Impersonate github-actions-sa@xml-bq-wos-analytics.iam.gserviceaccount.com
    │  short-lived OAuth2 access token
    ▼
Artifact Registry (push image)  +  GCS (write template spec)
```

Two **GitHub repository secrets** must be set (already configured):

| Secret | Value |
|--------|-------|
| `WIF_PROVIDER` | `projects/<project-number>/locations/global/workloadIdentityPools/github-actions-pool/providers/github-actions-provider` |
| `WIF_SERVICE_ACCOUNT` | `github-actions-sa@<project-id>.iam.gserviceaccount.com` |

### Setting up CI/CD in a new GCP project

If you fork this repo or set up a new environment, run the one-time setup script:

```bash
# Edit PROJECT_ID, GITHUB_ORG, GITHUB_REPO at the top of the script first
bash scripts/setup_wif.sh
```

The script:
1. Enables `iamcredentials.googleapis.com`
2. Creates the `github-actions-sa` service account with `artifactregistry.writer` + `storage.objectAdmin`
3. Creates the Workload Identity Pool and OIDC provider
4. Binds the SA to the pool, scoped to your repository only
5. Prints the two secret values to copy into GitHub

Then set the secrets:

```bash
gh secret set WIF_PROVIDER       --repo <org>/<repo> --body "<provider-resource-name>"
gh secret set WIF_SERVICE_ACCOUNT --repo <org>/<repo> --body "<sa-email>"
```

---

## Configuration

### XML Mapping Config (`parser/wos_config.xml`)

This file drives the entire parser. It maps XML node paths to `table:column` destinations:

```xml
<records>
  <REC>
    <static>
      <summary table="wos_summary:wos_summary">
        <pub_info>
          <pubyear>wos_summary:pubyear</pubyear>
          <vol>wos_summary:vol</vol>
        </pub_info>
      </summary>
    </static>
  </REC>
</records>
```

Config keys are constructed as `parent_tag/record_tag/...` — the default is `records/REC/...`. This must match the structure of your XML files.

### Schema Files

BigQuery schemas are generated once from the PostgreSQL source schema and committed to the repo:

```bash
python src/wos_beam_pipeline/utils/schema_generator.py \
  parser/wos_schema_final.sql \
  config/schemas
# Outputs: config/schemas/<table>_schema.json + config/schemas/all_schemas.json
```

`all_schemas.json` is uploaded to GCS by Terraform and referenced at pipeline runtime via `--schema_path`.

---

## BigQuery Tables

The pipeline writes to 46 normalized tables:

| Category | Key Tables |
|----------|-----------|
| Core | `wos_summary` |
| Authors | `wos_summary_names`, `wos_summary_names_email_addr` |
| Affiliations | `wos_addresses`, `wos_address_names`, `wos_address_organizations` |
| Publication | `wos_titles`, `wos_page`, `wos_publisher` |
| Classification | `wos_subjects`, `wos_headings`, `wos_keywords` |
| References | `wos_references` |
| Conference | `wos_conference`, `wos_conf_*` |
| Grants | `wos_grants`, `wos_grant_ids` |
| Abstracts | `wos_abstracts`, `wos_abstract_paragraphs` |

Verify row counts after a run:

```sql
SELECT table_id, row_count
FROM `<project>.wos_dev.__TABLES__`
ORDER BY row_count DESC;
```

---

## Dead Letter Queue (DLQ)

Records that fail parsing are written to GCS as JSON lines with enriched metadata:

```json
{
  "record_id": "WOS:001124170700001",
  "xml": "<REC>...</REC>",
  "error": "KeyError: 'records/REC/static/summary'",
  "error_type": "KeyError",
  "pipeline_step": "ParseXMLRecord",
  "timestamp": "2026-03-02T19:08:00.000Z"
}
```

**Location:** `gs://<project>-wos-dlq-<env>/failed_records/*.jsonl`

A zero-byte DLQ means all records parsed successfully. Check DLQ size first when debugging:

```bash
gsutil du -sh gs://<project>-wos-dlq-dev/failed_records/
```

---

## Monitoring

### Cloud Logging

```bash
# All logs for a job
gcloud logging read \
  'resource.type="dataflow_step" AND resource.labels.job_name="<job-name>"' \
  --freshness=2h

# Errors only
gcloud logging read \
  'resource.type="dataflow_step" AND severity>=ERROR' \
  --freshness=2h
```

### Dataflow Console

Navigate to **Dataflow → Jobs** in the GCP Console to see the pipeline graph, per-step throughput, and worker autoscaling.

### Diagnostic Patterns

| Symptom | Likely Cause |
|---------|-------------|
| Empty DLQ + empty BQ | XML split failed (check namespace / record tag) |
| Full DLQ + empty BQ | Config key mismatch (check `parent_tag`) |
| Empty DLQ + empty BQ (after parse fix) | `DoOutputsTuple` access issue in `main.py` |
| BQ load errors | Schema mismatch — REQUIRED field missing, or extra field not in schema |
| Pipeline exits immediately with dedup | File MD5 unchanged — file already registered. Expected behavior. |
| Dedup run: 0 rows written but counts doubled | Registry was empty on a re-run; check `wos_record_registry` row count |
| `TypeError: WriteToBigQuery.__init__()` | Wrong Beam version parameter — use `additional_bq_parameters` not `schema_update_options` |

---

## Development

### Running Tests

```bash
# Unit tests (no GCP needed)
pytest tests/unit/ -v

# Integration tests (require local XML sample)
pytest tests/integration/ -v

# E2E tests (require live GCP credentials)
pytest tests/e2e/ -v

# Coverage
pytest tests/unit/ --cov=src/wos_beam_pipeline --cov-report=html
```

### Making Changes

1. Create a branch and open a PR — `test.yml` runs automatically
2. Merge to `main` — `deploy.yml` rebuilds the Docker image and template spec if pipeline files changed
3. Launch a new job using **Method 3** above to test the updated template

### Manual Docker Build (local testing)

```bash
# Build for the correct platform (amd64, required by Dataflow)
docker build --platform linux/amd64 -t wos-xml-to-bq:local .

# Test the image entrypoint
docker run --rm wos-xml-to-bq:local python -c "from wos_beam_pipeline.main import run; print('OK')"
```

---

## Cost Estimation

**Per 778 MB file (22,659 records, ~12 minutes):**

| Resource | Cost |
|----------|------|
| Dataflow compute (10 workers × 1.26 h) | ~$2.50 |
| BigQuery batch load | Free |
| GCS storage | Negligible |
| **Total** | **~$2.50/file** |

**With preemptible workers:** ~60% discount → ~$1/file

**Monthly (100 files):** ~$100 optimized

---

## Troubleshooting

**`no matches found: *.xml`**
Shell glob expansion in zsh. Single-quote all `--parameters` values:
`--parameters 'input_pattern=gs://bucket/data/*.xml'`

**`ImportError: attempted relative import`**
The Flex Template launcher runs `FLEX_TEMPLATE_PYTHON_PY_FILE` as a plain script.
`launcher.py` (at repo root) resolves this by using absolute imports after `pip install -e .`.

**`Permission denied` pulling image on Dataflow**
Grant `roles/artifactregistry.reader` to the Dataflow service account on the `wos-pipeline` Artifact Registry repository.

**`IAM Service Account Credentials API disabled`**
Required for Workload Identity Federation token exchange:
`gcloud services enable iamcredentials.googleapis.com`

**`Identity Pool does not exist` in WIF IAM binding**
WIF IAM bindings require the numeric project number, not the project ID string.
Use `gcloud projects describe <project-id> --format='value(projectNumber)'`.

**`POSIX regex` error in Flex Template build**
Dataflow validates `metadata.json` regexes as POSIX ERE. Place `-` at the end of character classes: `[a-z0-9_-]` not `[a-z0-9-_]`.

---

## License

MIT License — see `LICENSE` for details.
