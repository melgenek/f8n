.DEFAULT_GOAL := build

.PHONY: build
PLATFORMS = linux/arm64
build:
	docker buildx build --progress=plain \
		--platform=$(PLATFORMS) \
		--target=multi-arch-package \
		-t multi-arch-build:latest .

.PHONY: e2e-test
e2e-test:
	docker compose up -d --force-recreate --build
	./tests/test.sh
