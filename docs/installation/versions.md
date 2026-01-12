# Versions

BanyanDB releases have different API versions and Release versions. To keep the compatibility with SkyWalking
OAP releases, you need to pick the proper API versions.

| API Version | BanyanDB Release Version |
|-------------|--------------------------|
| 0.9         | 0.9                      |
| 0.8         | 0.8                      |

SkyWalking OAP release(since 10.4.0) includes the compatible BanyanDB API versions number could be found in `/config/bydb.yml` and environment variable:
```
${SW_STORAGE_BANYANDB_COMPATIBLE_SERVER_API_VERSIONS}
```

You can select the latest release version of the required APIs to ensure maximum performance and access to the latest
features.
