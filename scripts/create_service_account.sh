gcloud iam service-accounts create dataproc-tennis-etl \
    --display-name="Dataproc Tennis ETL Service Account" \
    --description="Service account for spark jobs writing to bigquery"

SA_EMAIL="dataproc-tennis-etl@tennis-etl-pipeline.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding tennis-etl-pipeline \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding tennis-etl-pipeline \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding tennis-etl-pipeline \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectViewer"

gcloud iam service-accounts keys create ./keys/dataproc-sa-key.json \
    --iam-account="${SA_EMAIL}"

echo "Service account key saved to ./keys/dataproc-sa-key.json"