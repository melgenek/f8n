#!/bin/bash
set -e

test-conformance() {
  docker rm -f kubeconformance || true
  docker run --rm \
    --name kubeconformance \
    --network container:$(docker ps -q -f name=k3s) \
    -e "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" \
    -e E2E_FOCUS="sig-api-machinery" \
    -e E2E_SKIP="Flaky" \
    -e E2E_EXTRA_ARGS="--ginkgo.fail-fast" \
    -v kubeconfig:/etc/rancher/k3s:ro \
    --entrypoint /usr/local/bin/kubeconformance \
    registry.k8s.io/conformance:v1.29.4
    # -e "E2E_FOCUS=should\snot\sbe\sblocked\sby\sdependency\scircle" \
}

test-conformance
