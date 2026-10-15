# Canopy

Canopy is a standalone web console for SkyWalking BanyanDB — a React SPA served by a Node.js BFF (Fastify).

## Dev setup

Two terminals:

**Terminal 1 — BFF server**
```
cp .env.example .env
# edit .env: set SESSION_SECRET, CANOPY_USERS or CANOPY_DEV_NOAUTH=true
npm install
CANOPY_DEV_NOAUTH=true npm run -w server dev
```

**Terminal 2 — React SPA**
```
npm run -w web dev
# open http://localhost:5173
```

## Production

```
npm run build
BANYANDB_TARGET=http://<host>:17913 SESSION_SECRET=<secret> CANOPY_USERS=/etc/canopy/users.yaml \
  node server/dist/src/index.js
# open http://localhost:4000
```

### Apache release tarball

The `skywalking-banyandb-*-canopy.tgz` archive contains the built `web/dist` and `server/dist` trees plus package manifests and Canopy license texts. It does not include `node_modules`. After unpacking:

```bash
npm ci --omit=dev
cd server
BANYANDB_TARGET=http://<host>:17913 SESSION_SECRET=<secret> CANOPY_USERS=/etc/canopy/users.yaml \
  node dist/src/index.js
```

### Reverse proxy path prefix

To expose Canopy below a shared host path, set `CANOPY_BASE_PATH` and preserve
that path when proxying requests:

```bash
CANOPY_BASE_PATH=/canopy
```

For the BanyanDB Helm chart, pass it through the existing `canopy.env` value.
Route `/canopy` to the Canopy service without an nginx rewrite or a trailing
slash on `proxy_pass`. The root `/healthz` endpoint remains available for
container and Kubernetes probes.

## Environment variables

See `.env.example` for all env vars.

- `BANYANDB_TARGET` — BanyanDB upstream the BFF proxies to (HTTP grpc-gateway, NOT :17912 gRPC). Single source of truth; the login form no longer accepts an override.
- `SESSION_SECRET` — **required** — random string for session cookie encryption
- `CANOPY_USERS` — **required** in production — path to JSON/YAML user list
- `CANOPY_DEV_NOAUTH=true` — dev only, seeds admin/admin; fails boot in NODE_ENV=production
- `BANYANDB_UPSTREAM_USERNAME/PASSWORD` — attach-if-configured service credential for every upstream request

## Monitor

The **Monitor** sidebar entry opens cluster routing and storage observations. It reads
`/api/v1/cluster/state` from `BANYANDB_TARGET` and Prometheus `/metrics` from
`MONITOR_TARGET` through the authenticated BFF. Administrators can view the page;
set `CANOPY_MONITOR_ALLOW_READONLY=true` to also permit readonly accounts.

The page refreshes every 10 seconds while visible. Set
`CANOPY_MONITOR_REFRESH_INTERVAL_MS` to an integer from 5000 to 300000 to change
the interval. Invalid values stop startup with a configuration error. Polling stops
when the page is hidden or closed; **Refresh** requests a new observation manually.

For a cluster-wide metrics view, point `MONITOR_TARGET` at the
[FODC proxy](../docs/operation/fodc/overview.md), which adds per-node `pod_name`
and `container_name` labels. A direct BanyanDB metrics endpoint reports only that
target, not the whole cluster. Unlabeled samples are identified as the configured
target. Disk paths may share one physical filesystem: their capacities must not be
summed to estimate fleet capacity.

Cluster states describe routing membership, not a separate health probe. API and
file-format versions are displayed when the node reports them; they are not the
application release version. Uptime is shown when the metrics source reports its
`banyandb_system_up_time` gauge; it is not inferred from creation timestamps.
A standalone server can report no cluster routes.
An unavailable source is shown independently, and a failed refresh marks the
last observation as stale. Missing disk values are shown as unavailable, not zero.

Query, event, and trace pipeline sections are hidden by default. Setting
`CANOPY_MONITOR_EXPERIMENTAL=true` shows placeholders only; these sections do
not yet collect data. See the [Monitor design](docs/monitor-design.md) for the
data contract, limits, and verification commands.
