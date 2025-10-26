.PHONY: initgke
initgke:
	terraform -chdir=terraform-gke init

.PHONY: plangke
plangke: initgke
	terraform -chdir=terraform-gke plan

.PHONY: applygke
applygke: initgke
	terraform -chdir=terraform-gke apply

.PHONY: destroygke
destroygke: initgke
	terraform -chdir=terraform-gke destroy -auto-approve

.PHONY: logingke
logingke:
	@echo "Logging into Kubernetes..."
	@bash -c "$$(terraform -chdir=terraform-gke output -raw kubeconfig)"

.PHONY: initvultr
initvultr:
	terraform -chdir=terraform-vultr init

.PHONY: planvultr
planvultr: initvultr
	terraform -chdir=terraform-vultr plan

.PHONY: applyvultr
applyvultr: initvultr
	terraform -chdir=terraform-vultr apply -auto-approve

.PHONY: destroyvultr
destroyvultr: initvultr
	terraform -chdir=terraform-vultr destroy -auto-approve

.PHONY: loginvultr
loginvultr:
	@echo "Logging into Kubernetes..."
	terraform -chdir=terraform-vultr output -raw kube_config | base64 --decode > ~/.kube/config

.PHONY: apply-openebs
apply-openebs:
	@echo "Applying openebs..."
	#helm repo add openebs https://openebs.github.io/openebs
	#helm repo update


	helm upgrade --install openebs openebs/openebs --namespace openebs --create-namespace --values ./resources/values-localebs.yaml

.PHONY: apply-gke-nvme
apply-gke-nvme:
	@echo "Applying NVMe SSD..."
	kubectl apply -f ./resources/gke-nvme

.PHONY: apply-fdb
apply-fdb:
	@echo "Applying FoundationDB cluster..."
	kubectl apply -f ./resources/fdb.yaml

.PHONY: apply-fdb-exporter
apply-fdb-exporter:
	@echo "Applying FoundationDB exporter..."
	kubectl apply -f ./fdb-exporter/manifests

.PHONY: apply-monitoring
apply-monitoring:
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	helm upgrade --install monitoring prometheus-community/kube-prometheus-stack --namespace monitoring --create-namespace --values ./resources/monitoring/values.yaml

.PHONY: apply-k8s
apply-k8s: apply-monitoring apply-fdb apply-fdb-exporter
	@echo "Applying K8S..."

.PHONY: proxy-grafana
proxy-grafana:
	@echo "Grafana admin password:"
	@kubectl -n monitoring get secret monitoring-grafana -o jsonpath="{.data.admin-password}" | base64 --decode; echo
	@export POD_NAME=$$(kubectl -n monitoring get pods -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=monitoring" -o jsonpath="{.items[0].metadata.name}"); \
	echo "Forwarding Grafana pod $$POD_NAME on http://localhost:3000"; \
	kubectl port-forward -n monitoring $$POD_NAME 3000:3000

.PHONY: apply-f8n
apply-f8n:
	@echo "Applying F8N..."
	kubectl apply -f ./resources/f8n

.PHONY: apply-etcd-benchmark
apply-etcd-benchmark:
	@echo "Applying Etcd benchmark..."
	kubectl apply -f ./etcd-benchmark/manifests

.PHONY: apply-lease-flood
apply-lease-flood:
	@echo "Applying lease flood..."
	kubectl apply -f ./etcd-lease-flood/manifests
