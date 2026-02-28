gcloud projects add-iam-policy-binding tennis-etl-pipeline \
    --member="serviceAccount:airflow-gcs-uploader@tennis-etl-pipeline.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding tennis-etl-pipeline \
    --member="serviceAccount:airflow-gcs-uploader@tennis-etl-pipeline.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"