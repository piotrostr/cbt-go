#!/bin/bash

instance_id=iot-example-instance
cluster_id=cpu-intensive-cluster
region=us-central1

gcloud bigtable instances delete $instance_id \
    --quiet \
    --async

gcloud container clusters delete $cluster_id \
    --region $region \
    --quiet \
    --async
