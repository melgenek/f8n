.PHONY: inittf
inittf:
	terraform -chdir=terraform-vultr init

.PHONY: plantf
plantf: inittf
	terraform -chdir=terraform-vultr plan

.PHONY: applytf
applytf: inittf
	terraform -chdir=terraform-vultr apply -auto-approve

.PHONY: destroytf
destroytf: inittf
	terraform -chdir=terraform-vultr destroy -auto-approve

.PHONY: logink8s
logink8s:
	@echo "Logging into Kubernetes..."
	terraform -chdir=terraform-vultr output -raw kube_config | base64 --decode > ~/.kube/config

.PHONY: apply-openebs
apply-openebs:
	@echo "Applying openebs..."
	helm repo add openebs https://openebs.github.io/openebs
	helm repo update
	helm upgrade --install openebs openebs/openebs --namespace openebs --values ./resources/values-localebs.yaml

.PHONY: apply-metrics-server
apply-metrics-server:
	@echo "Applying metrics-server..."
	kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml


.PHONY: apply-fdb-operator
apply-fdb-operator:
	@echo "Applying FoundationDB Operator manifests..."
	kubectl apply -f ./resources/foundationdb/cluster_crd.yaml
	kubectl apply -f ./resources/foundationdb/backups_crd.yaml
	kubectl apply -f ./resources/foundationdb/restores_crd.yaml
	kubectl apply -f ./resources/foundationdb/operator.yaml

.PHONY: apply-fdb
apply-fdb:
	@echo "Applying FoundationDB cluster..."
	kubectl apply -f ./resources/foundationdb/cluster.yaml

.PHONY: apply-k8s
apply-k8s: apply-openebs apply-fdb-operator apply-fdb
	@echo "Applying K8S..."

.PHONY: apply-f8n
apply-f8n:
	@echo "Applying F8N..."
	kubectl apply -f ./resources/f8n.yaml

.PHONY: apply-lease-flood
apply-lease-flood:
	@echo "Applying lease flood..."
	kubectl apply -f ./resources/etcd-lease-flood.yaml
