#!/bin/bash

instance_id=iot-example-instance
cluster_id=iot-example-cluster
zone=us-central1-a

time gcloud bigtable instances create $instance_id \
    --display-name $instance_id \
    --cluster-config id=$cluster_id,zone=$zone,nodes=1 \
    --cluster-storage-type SSD
