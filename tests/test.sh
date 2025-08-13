#!/bin/bash
set -e


wait-for-k3s-ready() {
  until docker exec k3s test -f /etc/rancher/k3s/k3s.yaml; do
    echo "Waiting for /etc/rancher/k3s/k3s.yaml..."
    sleep 1
  done
  echo "k3s.yaml is ready."

  until docker exec k3s kubectl get pods -A &>/dev/null; do
    echo "Waiting for pods..."
    sleep 1
  done
  echo "kubectl is ready."
}

test-load() {
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  docker build -t f8n:test-load --progress=plain -f "${SCRIPT_DIR}/load/Dockerfile" "${SCRIPT_DIR}/load"
  docker run --rm \
    --name f8n-test-load \
    --network container:$(docker ps -q -f name=k3s) \
    -v kubeconfig:/etc/rancher/k3s:ro \
    -e "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" \
    f8n:test-load
}

test-conformance() {
  docker rm -f kubeconformance || true
  docker run --rm \
    --name kubeconformance \
    --network container:$(docker ps -q -f name=k3s) \
    -e "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" \
    -e E2E_FOCUS="sig-api-machinery" \
    -e E2E_SKIP="StorageVersionAPI|Flaky" \
    -e E2E_EXTRA_ARGS="--ginkgo.fail-fast" \
    -v kubeconfig:/etc/rancher/k3s:ro \
    --entrypoint /usr/local/bin/kubeconformance \
    registry.k8s.io/conformance:v1.29.4
    # -e "E2E_FOCUS=should\snot\sbe\sblocked\sby\sdependency\scircle" \
}

wait-for-k3s-ready
test-load
test-conformance
