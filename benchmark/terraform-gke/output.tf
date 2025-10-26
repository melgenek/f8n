output "kubeconfig" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.test.name} --region ${google_container_cluster.test.location} --project ${var.project_id}"
}
# gcloud container clusters get-credentials test-cluster --region us-central1 --project pure-coda-475818-p7
output "cluster_location" {
  value = google_container_cluster.test.location
}

output "project_id" {
  value = var.project_id
}

output "cluster_name" {
  value = google_container_cluster.test.name
}
