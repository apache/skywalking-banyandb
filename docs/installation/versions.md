# Versions

BanyanDB releases have different API versions and Release versions. To keep the compatibility with SkyWalking
OAP releases, you need to pick the proper API versions.

| API Version | BanyanDB Release Version |
|-------------|--------------------------|
| 0.9         | 0.9                      |
| 0.8         | 0.8                      |

SkyWalking OAP release(since 10.2.0) includes `/config/bydb.dependencies.properties` in the binary indicates required
API version and tested release version.

```yaml
# BanyanDB version is the version number of BanyanDB Server release.
# This is the bundled and tested BanyanDB release version
bydb.version=0.8
# BanyanDB API version is the version number of the BanyanDB query APIs
# OAP server has bundled implementation of BanyanDB Java client.
# Please check BanyanDB documentation for the API version compatibility.
# https://skywalking.apache.org/docs/skywalking-banyandb/next/installation/versions/
# Each `bydb.api.version` could have multiple compatible release version(`bydb.version`).
bydb.api.version=0.8
```

You can select the latest release version of the required APIs to ensure maximum performance and access to the latest
features.
