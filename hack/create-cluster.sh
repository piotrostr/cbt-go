#!/bin/bash

cluster_id=cbt-filler-cluster

gcloud container clusters create-auto $cluster_id \
    --region us-central1 \
    --enable-vertical-pod-autoscaling

gcloud container clusters get-credentials $cluster_id \
    --region us-central1
