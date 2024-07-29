# Installation On Kubernetes

To install BanyanDB on Kubernetes, you can use our Helm chart, which simplifies the deployment process.  You can find detailed installation instructions in [our official documentation](https://github.com/apache/skywalking-banyandb-helm/blob/master/README.md).

This step-by-step guide assumes you have a basic understanding of Kubernetes and Helm, the package manager for Kubernetes. If you're new to Helm, you might want to familiarize yourself with Helm basics before proceeding.

## Prerequisites

Before we begin, ensure you have the following:

1. **A Kubernetes Cluster**: You can use Minikube for a local setup, or any cloud provider like AWS, GCP, or Azure that supports Kubernetes.
2. **Helm 3**: Ensure Helm 3 is installed and configured on your machine. You can download it from [Helm's official website](https://helm.sh/).

## Step 1: Configure Helm to Use OCI

Since the BanyanDB Helm chart is hosted as an OCI chart in Docker Hub, you need to ensure your Helm is configured to handle OCI artifacts.

```shell
helm registry login registry-1.docker.io
```

You will be prompted to enter your Docker Hub username and password. This step is necessary to pull Helm charts from Docker Hub.

## Step 2: Install BanyanDB Using Helm 

- Create a namespace for BanyanDB:
```shell
kubectl create ns sw
```

- Install BanyanDB using the following Helm command:
```shell
helm install banyandb \
  oci://registry-1.docker.io/apache/skywalking-banyandb-helm \
  --version 0.2.0 \
  --set image.tag=0.6.1 \
  -n sw
```
This command installs the BanyanDB Helm chart with the specified version and image tag in the `sw` namespace in `cluster mode`.
You can customize the installation by setting additional values in the `--set` flag.

- Wait for the installation to complete. You can check the status of the pods using the following command:
```shell
kubectl get pod -n sw -w
```
```shell
NAME                       READY   STATUS    RESTARTS        AGE
banyandb-0                 1/1     Running   3 (6m38s ago)   7m7s
banyandb-1                 1/1     Running   0               5m6s
banyandb-2                 1/1     Running   0               4m6s
banyandb-885bc59d4-669lh   1/1     Running   3 (6m35s ago)   7m7s
banyandb-885bc59d4-dd4j7   1/1     Running   3 (6m36s ago)   7m7s
banyandb-etcd-0            1/1     Running   0               7m7s
```

- You can check the services using the following command:
```shell
kubectl get svc -n sw
```
```shell
NAME                     TYPE           CLUSTER-IP      EXTERNAL-IP   PORT(S)             AGE
banyandb-etcd            ClusterIP      10.96.33.132    <none>        2379/TCP,2380/TCP   5m
banyandb-etcd-headless   ClusterIP      None            <none>        2379/TCP,2380/TCP   5m
banyandb-grpc            ClusterIP      10.96.152.152   <none>        17912/TCP           5m
banyandb-http            LoadBalancer   10.96.137.29    <pending>     17913:30899/TCP     5m
```
The BanyanDB server would be listening on the `0.0.0.0:17912` to access gRPC requests. if no errors occurred.

At the same time, the BanyanDB server would be listening on the `0.0.0.0:17913` to access HTTP requests. if no errors occurred. The HTTP server is used for CLI and Web UI.

