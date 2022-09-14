#!/bin/bash

# based on https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity

gsa_name=gcp-cbt-admin
project_id=piotrostr-resources
gsa=gcp-cbt-admin@$project_id.iam.gserviceaccount.com
ksa=kube-cbt-admin
namespace=default
role=roles/bigtable.admin

# create kubernetes service account (KSA)
kubectl create serviceaccount kube-cbt-admin \
    --namespace $namespace

# create google service account (GSA)
gcloud iam service-accounts create $gsa_name \
  --project $project_id

# add GSA permissions
gcloud projects add-iam-policy-binding $project_id \
  --member "serviceAccount:$gsa" \
  --role $role

# add GSA workloadIdentityUser for KSA
gcloud iam service-accounts add-iam-policy-binding $gsa \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:piotrostr-resources.svc.id.goog[$namespace/$ksa]"

# annotate the KSA
kubectl annotate serviceaccount $ksa \
    --namespace $namespace \
    iam.gke.io/gcp-service-account=$gsa
