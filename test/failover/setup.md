# Setup the Cluster

## Provisioning the KinD cluster

```bash
kind create cluster --config kind.yaml

kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl patch -n kube-system deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
```

## Build BanyanDB and Load Image into KinD

```bash
make docker.build
kind load docker-image apache/skywalking-banyandb:latest
```

## Deploy BanyanDB

```bash
helm registry login registry-1.docker.io

helm install "failover-test" \
  oci://ghcr.io/apache/skywalking-banyandb-helm/skywalking-banyandb-helm \
  --version "0.0.0-973f59b" \
  -n "default" \
  --set image.repository=apache/skywalking-banyandb \
  --set image.tag=latest \
  --set standalone.enabled=false \
  --set cluster.enabled=true \
  --set cluster.liaison.replicas=1 \
  --set cluster.data.replicas=1 \
  --set etcd.enabled=true \
  --set etcd.replicaCount=1
```

## Deploy Data Generator

```bash
kubectl apply -f oap-pod.yaml
```

## Trigger Data Generation

```bash
make up_traffic
```