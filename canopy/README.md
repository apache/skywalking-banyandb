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
- `CANOPY_USERS` — **deprecated alias** for `auth.local.users`: path to a JSON/YAML user list. It keeps working, and setting both it and `auth.local.users` is an error; passwords are argon2id PHC strings, legacy bcrypt hashes still work with a startup warning. Produce an argon2id hash from `canopy/`: `node -e "require('hash-wasm').argon2id({password:'your-password',salt:require('crypto').randomBytes(16),parallelism:1,iterations:3,memorySize:19456,hashLength:32,outputType:'encoded'}).then(console.log)"`
- `CANOPY_AUTH_CONFIG` — path to a YAML/JSON file with an `auth:` section (see [Authentication](#authentication)).
- `CANOPY_AUTH_BACKEND` — `local` or `sso`; overrides `auth.backend`. Default: `local` when `CANOPY_USERS` or `CANOPY_DEV_NOAUTH` is set, otherwise `sso`. `local` with no users boots and refuses every login.
- `CANOPY_AUTH_SESSION_COOKIE_NAME` — overrides `auth.session.cookieName` (default `canopy_sid`).
- `CANOPY_AUTH_SESSION_TTL_MINUTES` — overrides `auth.session.ttlMinutes`, a positive whole number (default `60`, sliding idle expiry).
- `CANOPY_AUTH_SESSION_COOKIE_SECURE` — overrides `auth.session.cookieSecure`: `true`, `false` or `auto` (default `auto`, Secure when the request came over HTTPS).
- `CANOPY_AUTH_SSO_<ID>_CLIENT_SECRET` — client secret of the SSO provider with that `id` (upper-cased, non-alphanumerics as `_`), unless the provider sets `clientSecretEnv` or `clientSecretFile`.
- `CANOPY_AUTH_TOKENS_FILE` — alias for `auth.tokensFile` (see [API tokens](#api-tokens)).
- `CANOPY_DEV_NOAUTH=true` — dev only, seeds admin/admin; fails boot in NODE_ENV=production
- `BANYANDB_UPSTREAM_USERNAME/PASSWORD` — attach-if-configured service credential for every upstream request

## Authentication

Config keys live under `auth:` (and `auth.session:`) in the file named by `CANOPY_AUTH_CONFIG`; the table of keys, defaults and the design behind them is in [docs/auth-design.md](docs/auth-design.md). Environment variables above override the scalar keys.

Role mapping: `admin`, `maintainer` and `operator` map to Canopy `admin` (read and write); `readonly` and `viewer` map to `readonly` (GET/HEAD/OPTIONS only). Sessions are held in server memory, so they do not survive a restart: everyone must log in again after the BFF restarts. The users files (`CANOPY_USERS`, `auth.local.users` in `CANOPY_AUTH_CONFIG`) are re-read every few seconds, so removing a user or changing a role takes effect on the next request without a re-login.

### Local users

```yaml
auth:
  backend: local
  local:
    users:
      - username: admin
        passwordHash: "$argon2id$v=19$m=19456,t=3,p=1$..."   # see the hash command above
        role: admin
```

### SSO

`auth.backend: sso` signs users in through OIDC or OAuth2 providers (authorization code with PKCE, `state` and `nonce`). The client secret is never written in the config file: it comes from `CANOPY_AUTH_SSO_<ID>_CLIENT_SECRET`, from `clientSecretEnv` (another variable name) or from `clientSecretFile`. The callback URL to register with the provider is `https://<canopy-host>/<base-path>/auth/sso/<id>/callback` (set `redirectUri` on the provider if Canopy sits behind a proxy that hides the public URL).

`auth.sso.roles` decides who may sign in: an exact `roleByEmail` entry (case-insensitive) wins over a `roleByDomain` entry, which wins over `defaultRoles`. A user with no match is refused with 403; there is no default admin.

Generic OIDC (issuer discovery):

```yaml
auth:
  backend: sso
  sso:
    providers:
      - id: corp
        type: oidc
        name: Corp SSO
        issuer: https://idp.example.com/realms/main
        clientId: canopy
        # secret: export CANOPY_AUTH_SSO_CORP_CLIENT_SECRET=...
    roles:
      defaultRoles: []
      roleByEmail: { alice@example.com: admin }
      roleByDomain: { example.com: viewer }
```

GitHub (OAuth2, explicit endpoints):

```yaml
auth:
  backend: sso
  sso:
    providers:
      - id: github
        type: oauth2
        name: GitHub
        clientId: <github-oauth-app-client-id>
        clientSecretFile: /run/secrets/github-client-secret
        authorizeUrl: https://github.com/login/oauth/authorize
        tokenUrl: https://github.com/login/oauth/access_token
        userinfoUrl: https://api.github.com/user
        emailsUrl: https://api.github.com/user/emails
    roles:
      roleByEmail: { alice@example.com: admin }
```

Provider keys: `id`, `type` (`oidc`|`oauth2`), `name`, `clientId`, `clientSecretEnv`|`clientSecretFile`, `scopes` (default `openid email profile` for OIDC, `read:user user:email` for OAuth2), `redirectUri`, and `issuer` (OIDC) or `authorizeUrl`/`tokenUrl`/`userinfoUrl`/`emailsUrl` (OAuth2). `auth.sso.timeoutMs` (default `5000`) bounds every call to a provider. With `backend: sso`, an `Authorization: Bearer <token>` that is not a Canopy API token (`hzn_...`) is checked against the providers' userinfo endpoints and mapped through `auth.sso.roles`. Edits to `auth.sso.roles` and `auth.breakGlass` in the auth file apply within about 5 seconds.

### Break-glass

With `backend: sso`, `auth.breakGlass` lists local accounts that can sign in through `POST /auth/login` **only while every SSO provider is unreachable**. Otherwise the login answers the same 401 as a wrong password. Hashes are argon2id (bcrypt also verifies). Successes are logged at WARN with the username and client IP, failures at INFO without the password; the login page shows the form only while SSO is down.

```yaml
auth:
  backend: sso
  # sso: ... as above
  breakGlass:
    enabled: true
    users:
      - username: ops
        passwordHash: "$argon2id$v=19$m=19456,t=3,p=1$..."
        role: admin
```

### API tokens

`auth.tokensFile` (or `CANOPY_AUTH_TOKENS_FILE`) points to a YAML/JSON list of `{id, username, secretHash, description?}`; `secretHash` is the SHA-256 hex of the secret. A token is `hzn_<id>_<secret>` (ids contain letters, digits and `-`) and is sent as `Authorization: Bearer hzn_...`. A token names a user, never a role: the user's current role is re-resolved from the live config (cached 30 s on success, never on failure), so deleting the user (or changing their role) in the users file is picked up within about 35 s (the users file is re-read every 5 s; a failed re-read keeps the previous list). Adding or removing token entries in the tokens file needs a restart. Tokens never create a cookie session and cannot call `/auth/login` or `/auth/logout`.

Mint a token (after `npm run build`, from `canopy/server`); it prints the token once to stdout and appends the hash entry to the file:

```sh
node dist/src/mint-token.js --file tokens.yaml --id t1 --username alice --description "ci"
```
