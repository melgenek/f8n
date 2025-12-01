.PHONY: build-push
build-push:
	docker buildx build --progress=plain \
		--platform=linux/amd64 \
		-t fra.vultrcr.com/f8ntest/lease-flood:0.0.5 .
	docker push fra.vultrcr.com/f8ntest/lease-flood:0.0.5
