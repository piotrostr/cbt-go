resource "google_container_cluster" "cluster" {
  name               = "cpu-intensive-cluster"
  location           = var.region
  initial_node_count = 3

  vertical_pod_autoscaling {
    enabled = true
  }
}

resource "google_service_account" "gcr_sa" {
  account_id   = "gcr-sa"
  display_name = "GCR Service Account"
}

resource "google_project_iam_binding" "storage_admin" {
  role = "roles/storage.objectViewer"
  members = [
    "serviceAccount:${google_service_account.gcr_sa.email}"
  ]
}

resource "google_container_node_pool" "cpu_intensive" {
  name     = "cpu-intensive"
  cluster  = google_container_cluster.cluster.name
  location = var.region

  autoscaling {
    min_node_count = 1
    max_node_count = 50
  }

  node_config {
    service_account = google_service_account.gcr_sa.email
    spot            = true
    machine_type    = "c2d-highcpu-8"
  }
}
