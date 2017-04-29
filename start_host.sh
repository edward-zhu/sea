#!/usr/bin/env bash

export GOOGLE_APPLICATION_CREDENTIALS="/app/gcs-service-account.json"
export PYTHONPATH="/app"

echo "sync time..."
ntpdate time.google.com

echo "mounting gcs..."
mkdir /gcloud
gcsfuse --implicit-dirs misaki-data /gcloud

python -u -m search.start