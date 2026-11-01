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
- `SESSION_SECRET` — **required** — random string, at least 32 characters
- `CANOPY_USERS` — path to a JSON/YAML user list (alias for `auth.local.users`); passwords are argon2id PHC strings, legacy bcrypt hashes still work with a startup warning. Produce an argon2id hash from `canopy/`: `node -e "require('hash-wasm').argon2id({password:'your-password',salt:require('crypto').randomBytes(16),parallelism:1,iterations:3,memorySize:19456,hashLength:32,outputType:'encoded'}).then(console.log)"`
- `CANOPY_AUTH_CONFIG` — path to a YAML/JSON file with an `auth:` section (`backend: local`, `local.users`, `session.cookieName|ttlMinutes|cookieSecure`); `CANOPY_AUTH_BACKEND` and `CANOPY_AUTH_SESSION_COOKIE_NAME|TTL_MINUTES|COOKIE_SECURE` override it. `backend: local` with no users boots and refuses every login; `sso` is not yet supported. `session.ttlMinutes` must be a positive whole number of minutes.
- `CANOPY_AUTH_TOKENS_FILE` — alias for `auth.tokensFile` (see [API tokens](#api-tokens)).
- `CANOPY_DEV_NOAUTH=true` — dev only, seeds admin/admin; fails boot in NODE_ENV=production
- `BANYANDB_UPSTREAM_USERNAME/PASSWORD` — attach-if-configured service credential for every upstream request

## Authentication

Role mapping: `admin`, `maintainer` and `operator` map to Canopy `admin` (read and write); `readonly` and `viewer` map to `readonly` (GET/HEAD/OPTIONS only). Sessions are held in server memory, so they do not survive a restart: everyone must log in again after the BFF restarts. The users files (`CANOPY_USERS`, `auth.local.users` in `CANOPY_AUTH_CONFIG`) are re-read every few seconds, so removing a user or changing a role takes effect on the next request without a re-login.

### API tokens

`auth.tokensFile` (or `CANOPY_AUTH_TOKENS_FILE`) points to a YAML/JSON list of `{id, username, secretHash, description?}`; `secretHash` is the SHA-256 hex of the secret. A token is `hzn_<id>_<secret>` (ids contain letters, digits and `-`) and is sent as `Authorization: Bearer hzn_...`. A token names a user, never a role: the user's current role is re-resolved from the live config (cached 30 s on success, never on failure), so deleting the user (or changing their role) in the users file is picked up within about 35 s (the users file is re-read every 5 s; a failed re-read keeps the previous list). Adding or removing token entries in the tokens file needs a restart. Tokens never create a cookie session and cannot call `/auth/login` or `/auth/logout`.

Mint a token (after `npm run build`, from `canopy/server`); it prints the token once to stdout and appends the hash entry to the file:

```sh
node dist/src/mint-token.js --file tokens.yaml --id t1 --username alice --description "ci"
```
