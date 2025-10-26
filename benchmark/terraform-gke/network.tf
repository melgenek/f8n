resource "google_compute_network" "vpc" {
  name                    = "gke-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "gke-subnet"
  ip_cidr_range = "10.10.0.0/20"
  network       = google_compute_network.vpc.self_link
  region        = "us-central1"

  secondary_ip_range {
    range_name    = "pods-range"
    ip_cidr_range = "10.20.0.0/15"
  }

  secondary_ip_range {
    range_name    = "services-range"
    ip_cidr_range = "10.30.0.0/22"
  }
}
