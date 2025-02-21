# Scale Test

## Provisioning the KinD cluster

```bash
kind create cluster --config kind.yaml
```

## Build BanyanDB and Load Image into KinD

```bash
make docker.build
kind load docker-image apache/skywalking-banyandb:latest
```

## Deploy BanyanDB

```bash
helm registry login registry-1.docker.io

helm install "scale-test" \
  oci://ghcr.io/apache/skywalking-banyandb-helm/skywalking-banyandb-helm \
  --version "0.0.0-973f59b" \
  -n "default" \
  --set image.repository=apache/skywalking-banyandb \
  --set image.tag=latest \
  --set standalone.enabled=false \
  --set cluster.enabled=true \
  --set cluster.data.replicas=1 \
  --set etcd.enabled=true
```

## Deploy Data Generator

```bash
kubectl apply -f oap-pod.yaml
```

## Trigger Data Generation

```bash
make up_traffic
```

## Verify Route Table and Files on Disk

Liaison nodes contain the same route table:

```json
{
  "measure-default-0": "10.244.0.12:17912",
  "measure-minute-0": "10.244.0.12:17912",
  "measure-minute-1": "10.244.0.12:17912",
  "stream-browser_error_log-0": "10.244.0.12:17912",
  "stream-browser_error_log-1": "10.244.0.12:17912",
  "stream-default-0": "10.244.0.12:17912",
  "stream-log-0": "10.244.0.12:17912",
  "stream-log-1": "10.244.0.12:17912",
  "stream-segment-0": "10.244.0.12:17912",
  "stream-segment-1": "10.244.0.12:17912",
  "stream-zipkin_span-0": "10.244.0.12:17912",
  "stream-zipkin_span-1": "10.244.0.12:17912"
}
```

All shards are stored on the same node:

## Case 1: Scale Out Data Nodes

Set statefulset replicas to 2:

```bash
kubectl scale statefulset banyandb --replicas=2
```

Verify that the new node is added to the route table:

```json
{
  "measure-default-0": "10.244.0.12:17912",
  "measure-minute-0": "10.244.0.19:17912",
  "measure-minute-1": "10.244.0.12:17912",
  "stream-browser_error_log-0": "10.244.0.19:17912",
  "stream-browser_error_log-1": "10.244.0.12:17912",
  "stream-default-0": "10.244.0.19:17912",
  "stream-log-0": "10.244.0.12:17912",
  "stream-log-1": "10.244.0.19:17912",
  "stream-segment-0": "10.244.0.12:17912",
  "stream-segment-1": "10.244.0.19:17912",
  "stream-zipkin_span-0": "10.244.0.12:17912",
  "stream-zipkin_span-1": "10.244.0.19:17912"
}
```

Shards are distributed across the two nodes.

> Note: TopNAggregation result measure is distributed independently of its source measure. So you may see
> measure-minute-0 and measure-minute-1 on the same node. See https://github.com/apache/skywalking/issues/12526.

## Case 2: Increase Shard Count

Set the number of shards to 2:

```bash
bydbctl group update -f measure-default.yaml
```

Verify that the new shard is added to the route table:

```json
{
  "measure-default-0": "10.244.0.12:17912",
  "measure-default-1": "10.244.0.19:17912",
  "measure-minute-0": "10.244.0.12:17912",
  "measure-minute-1": "10.244.0.19:17912",
  "stream-browser_error_log-0": "10.244.0.12:17912",
  "stream-browser_error_log-1": "10.244.0.19:17912",
  "stream-default-0": "10.244.0.12:17912",
  "stream-log-0": "10.244.0.19:17912",
  "stream-log-1": "10.244.0.12:17912",
  "stream-segment-0": "10.244.0.19:17912",
  "stream-segment-1": "10.244.0.12:17912",
  "stream-zipkin_span-0": "10.244.0.19:17912",
  "stream-zipkin_span-1": "10.244.0.12:17912"
}
```

`measure-default` are distributed across the two shards.

## Case 3: Scale Out to A Extreme Large Cluster

Set statefulset replicas to 10:

```bash
kubectl scale statefulset banyandb --replicas=10
```

Deploy a new data generator:

```bash
kubectl apply -f oap-pod-xl.yaml
```

Verify that the new node is added to the route table:

```json
{
  "measure-default-0": "10.244.0.51:17912",
  "measure-default-1": "10.244.0.54:17912",
  "measure-default-2": "10.244.0.55:17912",
  "measure-default-3": "10.244.0.56:17912",
  "measure-default-4": "10.244.0.57:17912",
  "measure-default-5": "10.244.0.58:17912",
  "measure-default-6": "10.244.0.60:17912",
  "measure-default-7": "10.244.0.61:17912",
  "measure-default-8": "10.244.0.62:17912",
  "measure-default-9": "10.244.0.63:17912",
  "measure-minute-0": "10.244.0.51:17912",
  "measure-minute-1": "10.244.0.54:17912",
  "measure-minute-2": "10.244.0.55:17912",
  "measure-minute-3": "10.244.0.56:17912",
  "measure-minute-4": "10.244.0.57:17912",
  "measure-minute-5": "10.244.0.58:17912",
  "measure-minute-6": "10.244.0.60:17912",
  "measure-minute-7": "10.244.0.61:17912",
  "measure-minute-8": "10.244.0.62:17912",
  "measure-minute-9": "10.244.0.63:17912",
  "stream-browser_error_log-0": "10.244.0.51:17912",
  "stream-browser_error_log-1": "10.244.0.54:17912",
  "stream-browser_error_log-2": "10.244.0.55:17912",
  "stream-browser_error_log-3": "10.244.0.56:17912",
  "stream-browser_error_log-4": "10.244.0.57:17912",
  "stream-browser_error_log-5": "10.244.0.58:17912",
  "stream-browser_error_log-6": "10.244.0.60:17912",
  "stream-browser_error_log-7": "10.244.0.61:17912",
  "stream-browser_error_log-8": "10.244.0.62:17912",
  "stream-browser_error_log-9": "10.244.0.63:17912",
  "stream-default-0": "10.244.0.51:17912",
  "stream-default-1": "10.244.0.54:17912",
  "stream-default-2": "10.244.0.55:17912",
  "stream-default-3": "10.244.0.56:17912",
  "stream-default-4": "10.244.0.57:17912",
  "stream-default-5": "10.244.0.58:17912",
  "stream-default-6": "10.244.0.60:17912",
  "stream-default-7": "10.244.0.61:17912",
  "stream-default-8": "10.244.0.62:17912",
  "stream-default-9": "10.244.0.63:17912",
  "stream-log-0": "10.244.0.51:17912",
  "stream-log-1": "10.244.0.54:17912",
  "stream-log-2": "10.244.0.55:17912",
  "stream-log-3": "10.244.0.56:17912",
  "stream-log-4": "10.244.0.57:17912",
  "stream-log-5": "10.244.0.58:17912",
  "stream-log-6": "10.244.0.60:17912",
  "stream-log-7": "10.244.0.61:17912",
  "stream-log-8": "10.244.0.62:17912",
  "stream-log-9": "10.244.0.63:17912",
  "stream-segment-0": "10.244.0.51:17912",
  "stream-segment-1": "10.244.0.54:17912",
  "stream-segment-2": "10.244.0.55:17912",
  "stream-segment-3": "10.244.0.56:17912",
  "stream-segment-4": "10.244.0.57:17912",
  "stream-segment-5": "10.244.0.58:17912",
  "stream-segment-6": "10.244.0.60:17912",
  "stream-segment-7": "10.244.0.61:17912",
  "stream-segment-8": "10.244.0.62:17912",
  "stream-segment-9": "10.244.0.63:17912",
  "stream-zipkin_span-0": "10.244.0.51:17912",
  "stream-zipkin_span-1": "10.244.0.54:17912",
  "stream-zipkin_span-2": "10.244.0.55:17912",
  "stream-zipkin_span-3": "10.244.0.56:17912",
  "stream-zipkin_span-4": "10.244.0.57:17912",
  "stream-zipkin_span-5": "10.244.0.58:17912",
  "stream-zipkin_span-6": "10.244.0.60:17912",
  "stream-zipkin_span-7": "10.244.0.61:17912",
  "stream-zipkin_span-8": "10.244.0.62:17912",
  "stream-zipkin_span-9": "10.244.0.63:17912"
}
```
