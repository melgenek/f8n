.PHONY: start-fdb
start-fdb:
	docker compose -f docker-compose-fdb.yaml up --force-recreate -d
	docker exec -ti fdb-local fdbcli

.PHONY: start-k3s
start-k3s:
	docker compose -f docker-compose-k3s.yaml up --force-recreate -d
	docker exec -ti k3s-local sh

.PHONY: test-conformance
test-conformance:
	docker run --rm \
        --name kubeconformance \
        --network container:"$$(docker ps -q -f name=k3s-local)" \
        -e "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" \
        -e "E2E_FOCUS=against\s2\spvcs\swith\ssame\svolume\sattributes\sclass" \
        -e E2E_SKIP="FeatureGate" \
        -e E2E_EXTRA_ARGS="--ginkgo.fail-fast" \
        -v kubeconfig-local:/etc/rancher/k3s:ro \
        --entrypoint /usr/local/bin/kubeconformance \
        registry.k8s.io/conformance:v1.33.3
