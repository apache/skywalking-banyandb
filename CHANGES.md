Changes by Version
==================
Release Notes.

0.3.0
------------------

#### Features
- Support special characters in the metric selector of HPA metric adapter.
- Add the namespace to HPA metric name.

#### Chores
- Upgrade skywalking-cli dependency.

0.2.0
------------------

#### Features
- Introduce custom metrics adapter to SkyWalking OAP cluster for Kubernetes HPA autoscaling.
- Add RBAC files and service account to support Kubernetes coordination.
- Add default and validation webhooks to operator controllers.
- Add UI CRD to deploy skywalking UI server.
- Add Fetcher CRD to fetch metrics from other telemetry system, for example, Prometheus.

#### Chores
- Transform project layers to support multiple applications.
- Introduce unit test to verify the operator.

0.1.0
------------------

#### Features
- Add OAPServer CRDs and controller.

#### Chores
- Set up GitHub actions to build from sources, check code styles, licenses.
