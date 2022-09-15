resource "google_container_cluster" "cluster" {
  name               = "cpu-intensive-cluster"
  location           = var.region
  initial_node_count = 3
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
    preemptible  = true
    spot         = true
    machine_type = "c2d-highcpu-4"
  }
}
