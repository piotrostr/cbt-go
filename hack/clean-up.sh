#!/bin/bash

instance_id=my-instance-id
cluster_id=cbt-filler-cluster
region=us-central1

gcloud bigtable instances delete $instance_id \
    --quiet \
    --async

gcloud container clusters delete $cluster_id \
    --region $region \
    --quiet \
    --async
