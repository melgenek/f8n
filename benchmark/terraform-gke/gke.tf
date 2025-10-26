resource "google_container_cluster" "test" {
  name     = "test-cluster"
  # Cluster is zonal https://stackoverflow.com/a/75378781/7221823
  location = "us-central1-a"
  network    = google_compute_network.vpc.self_link
  subnetwork = google_compute_subnetwork.subnet.self_link

  remove_default_node_pool = true
  initial_node_count       = 1

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods-range"
    services_secondary_range_name = "services-range"
  }

  addons_config {
    http_load_balancing {
      disabled = true
    }
    gce_persistent_disk_csi_driver_config {
      enabled = false
    }
  }

  deletion_protection = false

  depends_on = [
    google_project_service.container,
    google_project_service.compute,
  ]
}

resource "google_container_node_pool" "node_pool1" {
  name       = "node-pool1"
  location   = google_container_cluster.test.location
  cluster    = google_container_cluster.test.name
  node_count = 1

  node_config {
    spot            = true
    machine_type    = "c3d-standard-4"
    disk_size_gb    = 20
    disk_type       = "hyperdisk-balanced"
    # ephemeral_storage_local_ssd_config {
    #   local_ssd_count = 1
    # }
  }

  depends_on = [
    google_container_cluster.test,
  ]
}

resource "google_container_node_pool" "node_pool2" {
  name       = "node-pool2"
  location   = google_container_cluster.test.location
  cluster    = google_container_cluster.test.name
  node_count = 1

  node_config {
    spot            = true
    machine_type    = "c4d-standard-8-lssd"
    disk_size_gb    = 20
    disk_type       = "hyperdisk-balanced"
    ephemeral_storage_local_ssd_config {
      local_ssd_count = 1
    }
  }

  depends_on = [
    google_container_cluster.test,
  ]
}
