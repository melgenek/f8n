.DEFAULT_GOAL := build

ARCH ?= $(shell go env GOARCH)
REPO ?= rancher
DEFAULT_BUILD_ARGS=--build-arg="REPO=$(REPO)" --build-arg="TAG=$(TAG)" --build-arg="ARCH=$(ARCH)" --build-arg="DIRTY=$(DIRTY)"
DIRTY := $(shell git status --porcelain --untracked-files=no)
ifneq ($(DIRTY),)
	DIRTY="-dirty"
endif

.PHONY: validate
validate:
	docker build \
		$(DEFAULT_BUILD_ARGS) --progress=plain --build-arg="SKIP_VALIDATE=$(SKIP_VALIDATE)" \
		--target=validate -f Dockerfile .

.PHONY: build
build:
	docker build \
		$(DEFAULT_BUILD_ARGS) --progress=plain --build-arg="DRONE_TAG=$(DRONE_TAG)"  --build-arg="CROSS=$(CROSS)" \
		-f Dockerfile -t rancher/kine:dev --target=binary .

.PHONY: multi-arch-build
PLATFORMS = linux/arm64
multi-arch-build:
	docker buildx build --progress=plain \
		--platform=$(PLATFORMS) \
		--target=multi-arch-package \
		-t multi-arch-build:latest .

.PHONY: build-test
build-test:
	 docker build -t kine:test-dev --progress=plain -f Dockerfile.test .

.PHONY: test-fdb
#test-fdb: build build-test
test-fdb:
	docker run --rm -i -e ARCH -e REPO -e TAG  -e DRONE_TAG -e IMAGE_NAME \
    -v /var/run/docker.sock:/var/run/docker.sock -v kine-cache:/go/src/github.com/melgenek/f8n/.cache \
    -v /tmp:/tmp \
    --network=host \
    --privileged kine:test-dev "./scripts/test fdb"
