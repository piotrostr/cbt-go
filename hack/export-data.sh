#!/bin/bash

project_id=piotrostr-resources
instance_id=my-instance-id
region=us-central1
bucket_name=gs://$project_id-$instance_id/
table_id=mobile-time-series

gcloud services enable \
    storage.googleapis.com \
    dataflow.googleapis.com

gcloud alpha storage buckets create $bucket_name

gcloud dataflow jobs run dump-gcbt \
    --gcs-location gs://dataflow-templates/latest/Cloud_Bigtable_to_GCS_Parquet \
    --region $region \
    --parameters \
bigtableProjectId=$project_id,\
bigtableInstanceId=$instance_id,\
bigtableTableId=$table_id,\
outputDirectory=$bucket_name,\
filenamePrefix=output-,\
numShards=3,
