resource "vultr_kubernetes" "test" {
  label    = "test-cluster"
  region   = "fra"
  version  = "v1.33.0+3"

  node_pools {
    label      = "operator-pool"
    plan       = "voc-c-2c-4gb-75s"
    node_quantity = 3
    auto_scaler = false
    min_nodes = 1
    max_nodes = 3
    labels = {
      node-role = "operators"
    }
  }
}

# resource "vultr_kubernetes_node_pools" "fdb_node_pool" {
#   cluster_id = vultr_kubernetes.test.id
#   label      = "fdb-pool"
#   plan       = "voc-c-2c-4gb-75s"
#   node_quantity = 2
#   auto_scaler = false
#   min_nodes = 2
#   max_nodes = 2
#   tag       = "fdb-pool"
#   labels = {
#     node-role = "fdb"
#   }
# }
