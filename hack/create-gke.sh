#!/bin/bash

cluster_id=cpu-intensive-cluster
region=us-central1

gcloud container clusters create-auto $cluster_id \
    --region $region \
    --enable-vertical-pod-autoscaling

gcloud container clusters get-credentials $cluster_id \
    --region $region
