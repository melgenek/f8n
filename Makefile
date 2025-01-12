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


.PHONY: build-test
build-test:
	 docker build -t kine:test-dev --progress=plain -f Dockerfile.test .


.PHONY: test-nats
#test-nats: build build-test
test-nats:
	docker run -i -e ARCH -e REPO -e TAG  -e DRONE_TAG -e IMAGE_NAME \
    -v /var/run/docker.sock:/var/run/docker.sock -v kine-cache:/go/src/github.com/k3s-io/kine/.cache \
    --privileged kine:test-dev "./scripts/test nats-embedded"


.PHONY: test-sqlite
#test-sqlite: build build-test
test-sqlite:
	docker run -i -e ARCH -e REPO -e TAG  -e DRONE_TAG -e IMAGE_NAME \
    -v /var/run/docker.sock:/var/run/docker.sock -v kine-cache:/go/src/github.com/k3s-io/kine/.cache \
    --privileged kine:test-dev "./scripts/test sqlite"

.PHONY: test-fdb
#test-fdb: build build-test
test-fdb:
	docker run -i -e ARCH -e REPO -e TAG  -e DRONE_TAG -e IMAGE_NAME \
    -v /var/run/docker.sock:/var/run/docker.sock -v kine-cache:/go/src/github.com/k3s-io/kine/.cache \
    -v /tmp:/tmp \
    --network=host \
    --privileged kine:test-dev "./scripts/test fdb"
