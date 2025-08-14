#!/bin/bash
set -e

test-load() {
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  docker build -t f8n:test-load --progress=plain -f "${SCRIPT_DIR}/Dockerfile" "${SCRIPT_DIR}"
  docker run --rm \
    --name f8n-test-load \
    --network container:$(docker ps -q -f name=k3s) \
    -v kubeconfig:/etc/rancher/k3s:ro \
    -e "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" \
    f8n:test-load
}

test-load
