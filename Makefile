.PHONY: build
PLATFORMS = linux/arm64
build:
	docker buildx build --progress=plain \
		--platform=$(PLATFORMS) \
		--target=multi-arch-package \
		-t multi-arch-build:latest .

.PHONY: start-k3s
start-k3s:
	docker compose up -d --force-recreate --build
	@bash -c '\
	until docker exec k3s test -f /etc/rancher/k3s/k3s.yaml; do \
		echo "Waiting for /etc/rancher/k3s/k3s.yaml..."; \
		sleep 1; \
	done; \
	echo "k3s.yaml is ready."; \
	until docker exec k3s kubectl get pods -A &>/dev/null; do \
		echo "Waiting for pods..."; \
		sleep 1; \
	done; \
	echo "kubectl is ready." \
	'

.PHONY: test-conformance
test-conformance:
	docker rm -f kubeconformance || true
	docker run --rm \
		--name kubeconformance \
		--network container:"$$(docker ps -q -f name=k3s)" \
		-e "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" \
		-e "E2E_FOCUS=sig-api-machinery" \
		-e E2E_SKIP="StorageVersionAPI|Flaky" \
		-e E2E_EXTRA_ARGS="--ginkgo.fail-fast" \
		-v kubeconfig-local:/etc/rancher/k3s:ro \
		--entrypoint /usr/local/bin/kubeconformance \
		registry.k8s.io/conformance:v1.29.4

.PHONY: test-conformance
test-conformance-flaky:
	docker rm -f kubeconformance || true
	docker run --rm \
		--name kubeconformance \
		--network container:"$$(docker ps -q -f name=k3s)" \
		-e "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" \
		-e "E2E_FOCUS=sig-api-machinery" \
		-e E2E_SKIP="StorageVersionAPI" \
		-e E2E_EXTRA_ARGS="--ginkgo.fail-fast" \
		-v kubeconfig-local:/etc/rancher/k3s:ro \
		--entrypoint /usr/local/bin/kubeconformance \
		registry.k8s.io/conformance:v1.29.4

.PHONY: test-load
test-load:
	./tests/load/test.sh

.PHONY: test-all
test-all: test-load test-conformance
