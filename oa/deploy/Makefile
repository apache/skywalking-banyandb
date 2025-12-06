# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

.PHONY: help deploy-sidecar deploy-daemonset undeploy-sidecar undeploy-daemonset \
        deploy-local test-metrics check-deployment

NAMESPACE ?= skywalking
IMAGE_TAG ?= latest

help: ## Display this help message
	@echo "eBPF Sidecar Deployment Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-25s %s\n", $$1, $$2}'

# Kubernetes Deployments
deploy-sidecar: ## Deploy BanyanDB with eBPF sidecar (sidecar pattern)
	@echo "Deploying BanyanDB with eBPF sidecar..."
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f kubernetes/banyandb-with-sidecar.yaml
	@echo "Waiting for pods to be ready..."
	kubectl wait --for=condition=ready pod -l app=banyandb -n $(NAMESPACE) --timeout=300s
	@echo "Deployment complete!"
	@echo ""
	@echo "Access metrics:"
	@echo "  kubectl port-forward -n $(NAMESPACE) banyandb-0 18080:18080"
	@echo "  curl http://localhost:18080/metrics"

deploy-daemonset: ## Deploy eBPF sidecar as DaemonSet (node-level monitoring)
	@echo "Deploying eBPF sidecar as DaemonSet..."
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f kubernetes/ebpf-sidecar-daemonset.yaml
	@echo "Waiting for DaemonSet to be ready..."
	kubectl rollout status daemonset/ebpf-sidecar -n $(NAMESPACE) --timeout=300s
	@echo "Deployment complete!"
	@echo ""
	@echo "Check pods:"
	@echo "  kubectl get pods -n $(NAMESPACE) -l app=ebpf-sidecar"

undeploy-sidecar: ## Remove BanyanDB with sidecar deployment
	@echo "Removing BanyanDB with sidecar..."
	kubectl delete -f kubernetes/banyandb-with-sidecar.yaml --ignore-not-found=true
	@echo "Undeployment complete!"

undeploy-daemonset: ## Remove eBPF sidecar DaemonSet
	@echo "Removing eBPF sidecar DaemonSet..."
	kubectl delete -f kubernetes/ebpf-sidecar-daemonset.yaml --ignore-not-found=true
	@echo "Undeployment complete!"

# Docker Compose
deploy-local: ## Start local development environment with Docker Compose
	@echo "Starting local development environment..."
	cd .. && docker-compose up -d
	@echo "Services started!"
	@echo ""
	@echo "Access services:"
	@echo "  BanyanDB:       http://localhost:17913"
	@echo "  eBPF Metrics:   http://localhost:8080/metrics"
	@echo "  Prometheus:     http://localhost:9092"
	@echo "  Grafana:        http://localhost:3000 (admin/admin)"

stop-local: ## Stop local development environment
	@echo "Stopping local development environment..."
	cd .. && docker-compose down
	@echo "Services stopped!"

# Testing and Verification
test-metrics: ## Test metrics endpoint (requires port-forward)
	@echo "Testing metrics endpoint..."
	@curl -s http://localhost:18080/metrics | grep -E "^ebpf_" | head -20
	@echo ""
	@echo "Metrics are available!"

check-deployment: ## Check deployment status
	@echo "Checking deployment status..."
	@echo ""
	@echo "=== Pods ==="
	kubectl get pods -n $(NAMESPACE) -l app=banyandb -o wide
	@echo ""
	@echo "=== Services ==="
	kubectl get svc -n $(NAMESPACE)
	@echo ""
	@echo "=== eBPF Sidecar Logs (last 20 lines) ==="
	kubectl logs -n $(NAMESPACE) -l app=banyandb -c ebpf-sidecar --tail=20 --prefix=true

logs-sidecar: ## View eBPF sidecar logs
	kubectl logs -n $(NAMESPACE) -l app=banyandb -c ebpf-sidecar -f

logs-banyandb: ## View BanyanDB logs
	kubectl logs -n $(NAMESPACE) -l app=banyandb -c banyandb -f

# Port Forwarding
port-forward-metrics: ## Port forward eBPF metrics (18080)
	@echo "Port forwarding eBPF metrics to localhost:18080..."
	kubectl port-forward -n $(NAMESPACE) banyandb-0 18080:18080

port-forward-banyandb: ## Port forward BanyanDB HTTP (17913)
	@echo "Port forwarding BanyanDB HTTP to localhost:17913..."
	kubectl port-forward -n $(NAMESPACE) banyandb-0 17913:17913

# Debugging
debug-shell: ## Open shell in eBPF sidecar container
	kubectl exec -it -n $(NAMESPACE) banyandb-0 -c ebpf-sidecar -- /bin/bash

debug-ebpf-maps: ## Show eBPF maps (requires bpftool in container)
	kubectl exec -it -n $(NAMESPACE) banyandb-0 -c ebpf-sidecar -- \
		sh -c "ls -la /sys/fs/bpf/ 2>/dev/null || echo 'bpftool not available'"

debug-kernel: ## Check kernel version and eBPF support
	@echo "Checking kernel version and eBPF support..."
	kubectl exec -n $(NAMESPACE) banyandb-0 -c ebpf-sidecar -- uname -r
	kubectl exec -n $(NAMESPACE) banyandb-0 -c ebpf-sidecar -- \
		cat /proc/sys/kernel/unprivileged_bpf_disabled 2>/dev/null || echo "N/A"

# Cleanup
clean: undeploy-sidecar undeploy-daemonset ## Remove all deployments
	@echo "Cleanup complete!"

clean-namespace: ## Delete entire namespace (WARNING: deletes all data)
	@echo "WARNING: This will delete the entire $(NAMESPACE) namespace and all data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		kubectl delete namespace $(NAMESPACE); \
		echo "Namespace deleted!"; \
	else \
		echo "Cancelled."; \
	fi
