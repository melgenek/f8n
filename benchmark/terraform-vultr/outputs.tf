output "kube_config" {
  value     = vultr_kubernetes.test.kube_config
  sensitive = true
}
