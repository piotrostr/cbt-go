resource "google_bigtable_instance" "iot_example_instance" {
  name = "iot-example-instance"

  cluster {
    cluster_id   = "iot-example-cluster"
    zone         = var.region
    num_nodes    = 1
    storage_type = "SSD"
  }
}
