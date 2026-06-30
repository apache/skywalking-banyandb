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
  node server/dist/index.js
# open http://localhost:4000
```

## Environment variables

See `.env.example` for all env vars.

- `BANYANDB_TARGET` — BanyanDB upstream the BFF proxies to (HTTP grpc-gateway, NOT :17912 gRPC). Single source of truth; the login form no longer accepts an override.
- `SESSION_SECRET` — **required** — random string for session cookie encryption
- `CANOPY_USERS` — **required** in production — path to JSON/YAML user list
- `CANOPY_DEV_NOAUTH=true` — dev only, seeds admin/admin; fails boot in NODE_ENV=production
- `BANYANDB_UPSTREAM_USERNAME/PASSWORD` — attach-if-configured service credential for every upstream request
