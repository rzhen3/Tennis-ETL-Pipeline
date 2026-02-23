#!/bin/bash

set -euo pipefail

PROJECT_ID="tennis-etl-pipeline"

echo "Enabling required GCP APIs for current project..."

# enable required APIs
APIS=(
    "dataproc.googleapis.com"
    "bigquery.googleapis.com"
    "storage.googleapis.com"
    "bigquerystorage.googleapis.com"
    "iam.googleapis.com"
)

for api in "${APIS[@]}"; do
    echo "Enabling ${api}..."
    gcloud services enable "${api}" --project="${PROJECT_ID}" --quiet
done


echo "APIs enabled."
echo ""

# setup service account: Dataproc job runner
DATAPROC_SA_NAME="dataproc-tennis-etl"
DATAPROC_SA_EMAIL="${DATAPROC_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Setting up dataproc job runner service account: ${DATAPROC_SA_EMAIL}"

# create account
gcloud iam service-accounts create "${DATAPROC_SA_NAME}" \
    --display-name="Dataproc Tennis ETL Job Runner" \
    --description="Runs PySpark jobs on Dataproc serverless - reads GCS, writes BigQuery" \
    --project="${PROJECT_ID}" 2>/dev/null || echo "(already exists)"

# setting bigquery.dataEditor (r/w table data)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DATAPROC_SA_EMAIL}" \
    --role="roles/bigquery.dataEditor" \
    --quiet

# setting bigquery.jobUser (performing queries)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DATAPROC_SA_EMAIL}" \
    --role="roles/bigquery.jobUser" \
    --quiet

# setting storage.objectViewer (reading CSVs from bucket)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DATAPROC_SA_EMAIL}" \
    --role="roles/storage.objectViewer" \
    --quiet

echo "Dataproc job runner roles granted."
echo ""


# setup service account: airflow orchestrator
# Used by airflow to submit dataproc batches, r/w to GCS, act as dataproc SA
AIRFLOW_SA_NAME="airflow-orchestrator"
AIRFLOW_SA_EMAIL="${AIRFLOW_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "setting up Airflow orchestrator service account: ${AIRFLOW_SA_EMAIL}"

# create SA
gcloud iam service-accounts create "${AIRFLOW_SA_NAME}" \
    --display-name="Airflow Orchestrator" \
    --description="Airflow workers: submit Dataproc batches, uploads to GCS, manages pipeline" \
    --project="${PROJECT_ID}" 2>/dev/null || echo "(already exists)"

# give dataproc.editor
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${AIRFLOW_SA_EMAIL}" \
    --role="roles/dataproc.editor" \
    --quiet

# give storage.objectAdmin 
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${AIRFLOW_SA_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --quiet

# give bigquery.dataViewer
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${AIRFLOW_SA_EMAIL}" \
    --role="roles/bigquery.dataViewer" \
    --quiet

# give bigquery.jobUser
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${AIRFLOW_SA_EMAIL}" \
    --role="roles/bigquery.jobUser" \
    --quiet

# IAM: permission to run Dataproc batches AS the Dataproc SA
gcloud iam service-accounts add-iam-policy-binding "${DATAPROC_SA_EMAIL}" \
    --member="serviceAccount:${AIRFLOW_SA_EMAIL}" \
    --role="roles/iam.serviceAccountUser" \
    --quiet

echo "Airflow orchestrator roles granted."
echo ""

# generate key files

KEYS_DIR="./keys"
mkdir -p "${KEYS_DIR}"

# dataproc SA key
DATAPROC_KEY="${KEYS_DIR}/dataproc-sa-key.json"
if [ ! -f "${DATAPROC_KEY}" ]; then
    echo "Generating key: ${DATAPROC_KEY}"
    gcloud iam service-accounts keys create "${DATAPROC_KEY}" \
        --iam-account="${DATAPROC_SA_EMAIL}"
else
    echo "Key already exists: ${DATAPROC_KEY} (skipping)"
fi

# airflow SA key
AIRFLOW_KEY="${KEYS_DIR}/airflow-sa-key.json"
if [ ! -f "${AIRFLOW_KEY}"]; then
    echo "Generating key: ${AIRFLOW_KEY}"
    gcloud iam service-accounts keys create "${AIRFLOW_KEY}" \
        --iam-account="${AIRFLOW_SA_EMAIL}"
else
    echo "Key already exists: ${AIRFLOW_KEY} (skipping)"
fi

echo ""



echo "════════════════════════════════════════════════════════════════════════"
echo "Setup complete!"
echo ""
echo "Service accounts:"
echo "  Dataproc job runner:    ${DATAPROC_SA_EMAIL}"
echo "  Airflow orchestrator:   ${AIRFLOW_SA_EMAIL}"
echo ""
echo "Key files:"
echo "  ${DATAPROC_KEY}"
echo "  ${AIRFLOW_KEY}"
echo ""
echo "Next steps:"
echo "  1. Update the Airflow connection to use the orchestrator key:"
echo "     - Set google_cloud_default key_path to /opt/airflow/keys/airflow-sa-key.json"
echo "     - OR set GOOGLE_APPLICATION_CREDENTIALS in docker-compose.yaml:"
echo "       GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/keys/airflow-sa-key.json"
echo ""
echo "  2. Ensure keys/ is in .gitignore (NEVER commit key files)"
echo ""
echo "  3. Restart Airflow:"
echo "       docker-compose down && docker-compose up -d"
echo "════════════════════════════════════════════════════════════════════════"