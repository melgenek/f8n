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
	helm upgrade --install openebs openebs/openebs --namespace openebs --create-namespace --values ./resources/values-localebs.yaml

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

.PHONY: apply-monitoring
apply-monitoring:
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	#kubectl create configmap custom-dashboards --from-file=benchmark/resources/monitoring/custom-dashboard.json --dry-run=client -o yaml | kubectl apply -f -
	helm upgrade --install monitoring prometheus-community/kube-prometheus-stack --namespace monitoring --create-namespace --values ./resources/monitoring/values.yaml

.PHONY: proxy-grafana
proxy-grafana:
	@echo "Grafana admin password:"
	@kubectl -n monitoring get secret monitoring-grafana -o jsonpath="{.data.admin-password}" | base64 --decode; echo
	@export POD_NAME=$$(kubectl -n monitoring get pods -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=monitoring" -o jsonpath="{.items[0].metadata.name}"); \
	echo "Forwarding Grafana pod $$POD_NAME on http://localhost:3000"; \
	kubectl -n monitoring port-forward $$POD_NAME 3000:3000

.PHONY: apply-f8n
apply-f8n:
	@echo "Applying F8N..."
	kubectl apply -f ./resources/f8n.yaml

.PHONY: apply-lease-flood
apply-lease-flood:
	@echo "Applying lease flood..."
	kubectl apply -f ./resources/etcd-lease-flood.yaml
