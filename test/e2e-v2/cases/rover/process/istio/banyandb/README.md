# Rover process Istio e2e (BanyanDB)

This case is ported from [apache/skywalking test/e2e-v2/cases/rover/process/istio](https://github.com/apache/skywalking/tree/master/test/e2e-v2/cases/rover/process/istio). It runs the same flow (Kind, Istio, SkyWalking Helm, Bookinfo, SkyWalking Rover) but uses **BanyanDB** as OAP storage instead of Elasticsearch.

- **CI-only**: The test provisions a Kind cluster, Istio, ECK is skipped, BanyanDB is deployed in-cluster, then SkyWalking and Rover are installed. It is not practical to run locally without sufficient resources and the same env (e.g. `TAG`, `SW_KUBERNETES_COMMIT_SHA`, `SW_ROVER_COMMIT`, `ISTIO_VERSION` from `test/e2e-v2/script/env`).
- **Behavior**: Setup and verify steps are aligned with upstream; only storage and BanyanDB deployment steps differ. Verification (service list, instance list, process list via swctl) should match upstream expectations for compatibility.
