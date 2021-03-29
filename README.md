Apache SkyWalking Cloud on Kubernetes
============

![](https://github.com/apache/skywalking-swck/workflows/Build/badge.svg?branch=master)

<img src="http://skywalking.apache.org/assets/logo.svg" alt="Sky Walking logo" height="90px" align="right" />

A bridge project between [Apache SkyWalking](https://github.com/apache/skywalking) and Kubernetes.

SWCK is a platform for the SkyWalking user, provisions, upgrades, maintains SkyWalking relevant components, and makes them work natively on Kubernetes. 

# Features

 1. Operator: Provision and maintain SkyWalking backend components.
 1. Custom Metrics Adapter: Provides custom metrics come from SkyWalking OAP cluster for autoscaling by Kubernetes HPA

# Quick Start

 * Go to the [download page](https://skywalking.apache.org/downloads/) to download latest release manifest. 

## Operator

 * To install the operator in an existing cluster, make sure you have [`cert-manager` installed](https://cert-manager.io/docs/installation/)
 * Apply the manifests for the Controller and CRDs in release/config:
 
 ```
 kubectl apply -f release/operator/config
 ```

For more details, please refer to [deploy operator](docs/operator.md)

### Examples of the Operator

There are some instant examples to represent the functions or features of the Operator.

 - [Deploy OAP server and UI with default settings](./docs/examples/default-backend.md)
 - [Fetch metrics from the Istio control plane(istiod)](./docs/examples/istio-controlplane.md)

## Custom Metrics Adapter
  
 * Deploy OAP server by referring to Operator Quick Start.
 * Apply the manifests for an adapter in release/adapter/config:
 
 ```
 kubectl apply -f release/adapter/config
 ```

For more details, please read [Custom metrics adapter](docs/custom-metrics-adapter.md)

# Contributing
For developers who want to contribute to this project, see [Contribution Guide](CONTRIBUTING.md)

# License
[Apache 2.0 License.](/LICENSE)
