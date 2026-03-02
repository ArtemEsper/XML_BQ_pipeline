#!/usr/bin/env bash
# =============================================================================
# setup_wif.sh — Configure Workload Identity Federation for GitHub Actions
#
# Run this script ONCE from your local machine with Owner/Editor permissions.
# It creates:
#   - A dedicated CI/CD service account
#   - A Workload Identity Pool + OIDC Provider for GitHub
#   - IAM bindings so GitHub Actions can impersonate the SA
#
# After running, add the two outputs as GitHub repository secrets:
#   WIF_PROVIDER   — the provider resource name
#   WIF_SERVICE_ACCOUNT — the SA email
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration — edit these if your setup differs
# ---------------------------------------------------------------------------
PROJECT_ID="xml-bq-wos-analytics"
REGION="us-central1"
GITHUB_ORG="ArtemEsper"               # GitHub username or org
GITHUB_REPO="XML_BQ_pipeline"         # GitHub repository name

# CI/CD service account (separate from the Dataflow runtime SA)
CI_SA_NAME="github-actions-sa"
CI_SA_EMAIL="${CI_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Workload Identity Pool + Provider
WIF_POOL_ID="github-actions-pool"
WIF_PROVIDER_ID="github-actions-provider"

# IAM WIF resource names must use the numeric project NUMBER, not the project ID.
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')
WIF_POOL_NAME="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${WIF_POOL_ID}"
WIF_PROVIDER_NAME="${WIF_POOL_NAME}/providers/${WIF_PROVIDER_ID}"

# Artifact Registry repository
AR_REPO="wos-pipeline"

# GCS bucket where the Flex Template spec lives
TEMPLATE_BUCKET="xml-bq-wos-analytics-dataflow-temp-dev"

# ---------------------------------------------------------------------------
echo "=== Step 1: Create CI/CD service account ==="
# ---------------------------------------------------------------------------
gcloud iam service-accounts create "${CI_SA_NAME}" \
  --project="${PROJECT_ID}" \
  --display-name="GitHub Actions CI/CD Service Account" \
  --description="Used by GitHub Actions to push Docker images and update Flex Template specs" \
  || echo "Service account already exists, continuing..."

# ---------------------------------------------------------------------------
echo "=== Step 2: Grant SA the required roles ==="
# ---------------------------------------------------------------------------

# Push Docker images to Artifact Registry
gcloud artifacts repositories add-iam-policy-binding "${AR_REPO}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --member="serviceAccount:${CI_SA_EMAIL}" \
  --role="roles/artifactregistry.writer"

# Write (and overwrite) the Flex Template spec JSON in GCS
gcloud storage buckets add-iam-policy-binding "gs://${TEMPLATE_BUCKET}" \
  --member="serviceAccount:${CI_SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

# ---------------------------------------------------------------------------
echo "=== Step 3: Create Workload Identity Pool ==="
# ---------------------------------------------------------------------------
gcloud iam workload-identity-pools create "${WIF_POOL_ID}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="GitHub Actions Pool" \
  || echo "Pool already exists, continuing..."

# ---------------------------------------------------------------------------
echo "=== Step 4: Create OIDC Provider for GitHub ==="
# ---------------------------------------------------------------------------
gcloud iam workload-identity-pools providers create-oidc "${WIF_PROVIDER_ID}" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="${WIF_POOL_ID}" \
  --display-name="GitHub Actions Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
  --attribute-condition="assertion.repository_owner == '${GITHUB_ORG}'" \
  || echo "Provider already exists, continuing..."

# ---------------------------------------------------------------------------
echo "=== Step 5: Allow GitHub Actions to impersonate the SA ==="
# Restrict to exactly this repository (not the whole org)
# ---------------------------------------------------------------------------
MEMBER="principalSet://iam.googleapis.com/${WIF_POOL_NAME}/attribute.repository/${GITHUB_ORG}/${GITHUB_REPO}"

gcloud iam service-accounts add-iam-policy-binding "${CI_SA_EMAIL}" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="${MEMBER}"

# ---------------------------------------------------------------------------
echo ""
echo "=== Setup complete! ==="
echo ""
echo "Add the following secrets to your GitHub repository"
echo "  (Settings → Secrets and variables → Actions → New repository secret):"
echo ""
echo "  Secret name : WIF_PROVIDER"
echo "  Secret value: projects/$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')/locations/global/workloadIdentityPools/${WIF_POOL_ID}/providers/${WIF_PROVIDER_ID}"
echo ""
echo "  Secret name : WIF_SERVICE_ACCOUNT"
echo "  Secret value: ${CI_SA_EMAIL}"
echo ""
echo "You can also set them via the gh CLI:"
echo "  gh secret set WIF_PROVIDER --body \"<value above>\""
echo "  gh secret set WIF_SERVICE_ACCOUNT --body \"${CI_SA_EMAIL}\""
