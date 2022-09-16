# gcbt

* Features Bigtable setup and sample usage in Go, running on GKE and writing
  data to Cloud Bigtable. 
* The `main.go` program simulates load of IOT devices, with 350 replicas of the
  Pod running with Vertical Pod Autoscaler in the Autopilot cluster, the load
  was similar to 350,000 devices. Running for just short of an hour
  ([chart](https://photos.app.goo.gl/dQmz6ns7eA7pqTUh6)) generated over 160GB
  of data.
* Exported data to Cloud Storage using Dataflow
* Remote build using Skaffold and Cloud Build
* Setup both with Terraform and GCloud `hack/` scripts

# Setup
To provision resources, either

```bash
terraform apply
```

or

```bash
./hack/create-gke.sh
./hack/create-cbt.sh
```

To create a service account to be used by pods

```bash
./hack/create-service-account.sh
```

# Teardown

Export data using 

```bash
./hack/export-data.sh
```

and destroy the resources using

```bash
terraform destroy
```

or 

```bash
./hack/clean-up.sh
```
