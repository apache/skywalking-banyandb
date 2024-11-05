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

### Standalone Mode
- This command installs the BanyanDB Helm chart with the specified version and image tag in the `sw` namespace in `standalone mode`.
  You can customize the installation by setting additional values in the `--set` flag.

```shell
helm install banyandb \
    oci://registry-1.docker.io/apache/skywalking-banyandb-helm \
    --version 0.3.0 \
    --set image.tag=0.7.0 \
    --set standalone.enabled=true \
    --set cluster.enabled=false \
    --set etcd.enabled=false \
    --set storage.enabled=true \
    -n sw
```

- Wait for the installation to complete. You can check the status of the pods using the following command:
```shell
kubectl get pod -n sw -w
```
```shell
NAME         READY   STATUS    RESTARTS   AGE
banyandb-0   1/1     Running   0          71s
```

- You can check the storage using the following command:
```shell
kubectl get pvc -n sw
```
```shell
NAME              STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   VOLUMEATTRIBUTESCLASS   AGE
data-banyandb-0   Bound    pvc-a15e18bf-0423-4ff7-8a09-c68686c3c880   50Gi       RWO            standard       <unset>                 2m12s
meta-banyandb-0   Bound    pvc-548cfe75-8438-4fe2-bce5-66cd7fda4ffc   5Gi        RWO            standard       <unset>                 2m12s
```
The default storage set the `stream` and `measure` data to the same PVC `data-banyandb` and the `meta` data to the PVC `meta-banyandb`.

- You can check the services using the following command:
```shell
kubectl get svc -n sw
```
```shell
NAME            TYPE           CLUSTER-IP     EXTERNAL-IP   PORT(S)           AGE
banyandb-grpc   ClusterIP      10.96.217.34   <none>        17912/TCP         12m
banyandb-http   LoadBalancer   10.96.90.175   <pending>     17913:30325/TCP   12m
```
The BanyanDB server would be listening on the `0.0.0.0:17912` to access gRPC requests. if no errors occurred.

At the same time, the BanyanDB server would be listening on the `0.0.0.0:17913` to access HTTP requests. if no errors occurred. The HTTP server is used for CLI and Web UI.

### Cluster Mode
- This command installs the BanyanDB Helm chart with the specified version and image tag in the `sw` namespace in `cluster mode`.
  You can customize the other additional values in the `--set` flag.

```shell
helm install banyandb \
    oci://registry-1.docker.io/apache/skywalking-banyandb-helm \
    --version 0.3.0 \
    --set image.tag=0.7.0 \
    --set standalone.enabled=false \
    --set cluster.enabled=true \
    --set etcd.enabled=true \
    --set storage.enabled=true \
    --set storage.persistentVolumeClaims[0].mountTargets[0]=stream \
    --set storage.persistentVolumeClaims[0].claimName=stream-data\
    --set storage.persistentVolumeClaims[0].size=10Gi\
    --set storage.persistentVolumeClaims[0].accessModes[0]=ReadWriteOnce\
    --set persistentVolumeClaims[0].volumeMode=Filesystem\
    --set storage.persistentVolumeClaims[1].mountTargets[0]=measure \
    --set storage.persistentVolumeClaims[1].claimName=measure-data\
    --set storage.persistentVolumeClaims[1].size=10Gi\
    --set storage.persistentVolumeClaims[1].accessModes[0]=ReadWriteOnce\
    --set persistentVolumeClaims[1].volumeMode=Filesystem\
  -n sw
```

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
In cluster model the default `cluster.liaison.replicas` is 2, and the default `cluster.data.replicas` is 3. The default `etcd.replicas` is 1.

- You can check the storage using the following command:
```shell
kubectl get pvc -n sw
```
```shell
NAME                      STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   VOLUMEATTRIBUTESCLASS   AGE
data-banyandb-etcd-0      Bound    pvc-b0139dc4-01ed-423d-8f00-13376ecad015   8Gi        RWO            standard       <unset>                 13m
measure-data-banyandb-0   Bound    pvc-471758b8-1dc9-4508-85cb-34f2e53ee4a0   10Gi       RWO            standard       <unset>                 13m
measure-data-banyandb-1   Bound    pvc-a95f7196-b55c-4030-a99a-96476716c3f4   10Gi       RWO            standard       <unset>                 11m
measure-data-banyandb-2   Bound    pvc-29983adc-ee40-45b5-944c-49379a13384c   10Gi       RWO            standard       <unset>                 10m
stream-data-banyandb-0    Bound    pvc-60f2f332-cc07-4a3d-aad7-6c7866cfb739   10Gi       RWO            standard       <unset>                 13m
stream-data-banyandb-1    Bound    pvc-66dad00a-2c16-45b3-8d0a-dbf939d479ac   10Gi       RWO            standard       <unset>                 11m
stream-data-banyandb-2    Bound    pvc-bfdfde2d-7e78-4e9f-b781-949963e729f3   10Gi       RWO            standard       <unset>                 10m
```
The command creates the PVCs for the `stream` and `measure` data in the different PVCs. You can customize them by setting the `storage.persistentVolumeClaims` in the `--set` flag.
In the cluster model, BanyanDB leverage the `etcd` to manage the metadata.

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

## Uninstall
- Uninstall BanyanDB using the following command:
```shell
helm uninstall banyandb -n sw
```

- If you want to delete the PVCs, you can use the following command:
**Note**: This will delete all data stored in BanyanDB.
```shell
kubectl delete pvc --all -n sw
```