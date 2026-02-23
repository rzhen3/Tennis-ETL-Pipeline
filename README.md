# Tennis-ETL-Pipeline

bootup:
docker-compose up -d

reminders:

setup google auth `gcloud auth application-default login`
init python venv `./tennis-etl-env/Scripts/activate`
login to postgres `psql -U postgres`

reminder for terraform:
enable gcloud services for dataproc, bigquery, storage, bigquerystorage ...

to do list:
- test artifact uploading to GCP bucket
  - debug upload_jobs_to_GCS_bucket: cannot serialize object of type posix path 
- create script to setup wheel and add as bashoperator
- run simple pyspark job to make sure it works
- setup pyspark script...
- (optional) setup terraform