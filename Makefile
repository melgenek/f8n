.DEFAULT_GOAL := build

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
	./tests/conformance/test.sh

.PHONY: test-load
test-load:
	./tests/load/test.sh

.PHONY: test-all
test-all: test-load test-conformance
