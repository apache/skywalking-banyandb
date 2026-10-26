<!--
  Licensed to Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for
  additional information regarding copyright ownership. Apache Software
  Foundation (ASF) licenses this file to you under the Apache License,
  Version 2.0 (the "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations
  under the License.
-->


# Canopy authentication design

Canopy authenticates browsers with server-side sessions and scripts with API tokens. The user
model, config schema and role names follow Horizon's, so a Horizon `auth` section moves over with
few changes. This page describes the whole `auth.*` / `session.*` surface; the README has
copy-paste examples.

## Config schema

The `auth` section lives in the YAML/JSON file named by `CANOPY_AUTH_CONFIG`. Environment
variables override the scalar keys.

| Key | Env override | Default | Meaning |
| --- | --- | --- | --- |
| `auth.backend` | `CANOPY_AUTH_BACKEND` | `local` when `CANOPY_USERS` or `CANOPY_DEV_NOAUTH` is set, otherwise `sso` | `local` or `sso`. |
| `auth.local.users[]` | `CANOPY_USERS` (file, alias) | none | `{username, passwordHash, role}`; argon2id PHC strings or bcrypt. |
| `auth.tokensFile` | `CANOPY_AUTH_TOKENS_FILE` | none | API tokens, see below. |
| `auth.session.cookieName` | `CANOPY_AUTH_SESSION_COOKIE_NAME` | `canopy_sid` | Session cookie name. |
| `auth.session.ttlMinutes` | `CANOPY_AUTH_SESSION_TTL_MINUTES` | `60` | Sliding idle expiry. |
| `auth.session.cookieSecure` | `CANOPY_AUTH_SESSION_COOKIE_SECURE` | `auto` | `true`, `false` or `auto` (Secure when the request came over HTTPS). |
| `auth.sso.providers[]` | none | none | Identity providers, see below. |
| `auth.sso.roles` | none | none | Role mapping, see below. |
| `auth.sso.timeoutMs` | none | `5000` | Bound on every call to an identity provider. |
| `auth.breakGlass` | none | none | `{enabled, users[]}`, see below. |

Provider keys: `id`, `type` (`oidc` or `oauth2`), `name`, `clientId`, `clientSecretEnv` or
`clientSecretFile`, `scopes`, `redirectUri`, and either `issuer` (OIDC) or `authorizeUrl`,
`tokenUrl`, `userinfoUrl` and optional `emailsUrl` (OAuth2). The client secret is never read
from the config file: a `clientSecret` key is a startup error. It comes from the environment
variable named by `clientSecretEnv` (default `CANOPY_AUTH_SSO_<ID>_CLIENT_SECRET`, with `<ID>`
upper-cased and non-alphanumerics turned into `_`) or from the file named by `clientSecretFile`.

Differences from Horizon: Canopy has only the `admin` and `readonly` roles. Horizon's `viewer`
maps to `readonly`, and `maintainer`, `operator` and `admin` map to `admin`. Canopy has no LDAP
backend and a single `auth.sso` block rather than several identity-provider sections.

## Resolution order

For every request Canopy tries, in order: an IdP bearer token, an API token
(`Authorization: Bearer hzn_<id>_<secret>`), then the session cookie. A bearer credential that
is recognised but refused ends the chain with 401.

- **IdP bearer** (only with `backend: sso`): any bearer credential that does not start with
  `hzn_` is presented to the userinfo endpoint of each configured provider (OIDC `userinfo_endpoint`
  from discovery, or the OAuth2 `userinfoUrl`). The first provider that accepts it identifies the
  caller by email, and the role comes from `auth.sso.roles`. A token no provider accepts is
  refused with 401. Success is cached for 30 seconds per token (the role mapping is still applied
  live on every request); failures are never cached. Userinfo does not tell which client a token was
  issued for, so every access token that the provider accepts counts; scope the provider
  accordingly. An IdP bearer token never creates a cookie session.
- **API token**: see below; `hzn_` credentials are never sent to an identity provider.
- **Cookie**: the server-side session.

Tokens cannot call `/auth/login`.

## Roles, re-resolution and caching

Neither a session nor a token stores a role. The role is worked out again on each request from
the live configuration:

- local sessions look the username up in the users files, which are re-read every 5 seconds;
- SSO sessions and IdP bearer tokens apply `auth.sso.roles` to the email they were created with;
- break-glass sessions look the username up in `auth.breakGlass.users`, and only while break-glass is enabled;
- API tokens look their user up in the users files, and the result is cached for 30 seconds
  on success. A failure is never cached, so a user who is added back works at once.

The users files, `auth.sso.roles` and `auth.breakGlass` are re-read from `CANOPY_AUTH_CONFIG` at most
every 5 seconds (a failed re-read keeps the previous values). Removing a user or role, or disabling
break-glass, therefore revokes existing sessions within about 5 seconds, and within about 35 seconds
for API tokens, without a restart. Provider settings (`auth.sso.providers`, `timeoutMs`) need a restart.

## SSO

`auth.backend: sso` uses the authorization-code flow with PKCE (S256), `state` and, for OIDC,
`nonce`.

1. `GET /auth/sso/:provider/start` records the pending login (state, nonce, PKCE verifier) in a
   bounded in-memory map with a 10 minute lifetime, sets a short-lived `<cookieName>_sso` cookie
   holding the state, and redirects to the provider.
2. `GET /auth/sso/:provider/callback` consumes the state exactly once and requires it to equal
   the cookie, so a wrong, expired or replayed state (and a replayed code) answers 400 before the
   provider is contacted. It exchanges the code, then resolves the identity.
3. OIDC: the code is exchanged with the client authentication method the provider's discovery
   document supports (`client_secret_post` when listed, otherwise `client_secret_basic`, the OIDC
   default). The ID token signature is checked against the provider's JWKS (RS256 and ES256 only,
   never `none`), along with `iss`, `aud`, `azp` (whenever present, and required with several
   audiences), `exp`, `nbf`, `nonce` and `sub`. The JWKS is cached for 10 minutes and refetched once
   when a signature does not verify, so rotated keys are picked up and retired keys stop being
   trusted. A failure answers 401 `invalid_id_token`. The email comes from the token, or from the
   userinfo endpoint (only when its `sub` matches). An email marked `email_verified: false` is never
   used. OAuth2: the email comes from the userinfo endpoint (unless marked unverified) or, when it
   has none, the primary verified address from `emailsUrl` (GitHub's `/user/emails`).
4. `auth.sso.roles` picks the role: an exact `roleByEmail` entry (case-insensitive) wins over a
   `roleByDomain` entry, which wins over `defaultRoles`. With no match the login is refused with
   403 and no session; there is no default admin. When a match lists several roles, `admin` wins.
5. On success Canopy creates a session and redirects to the console.

### Username as the credential anchor

A session and a token name a username and nothing else. For SSO the username is the lower-cased
email (or `<provider>:<login>` when the provider gave no email). The role is recomputed from that
anchor, never taken from a stored value.

### SSO health

`GET /api/meta` reports `auth.backend`, the configured providers, `ssoHealthy` and whether
break-glass is enabled. A provider is healthy when its OIDC discovery document (or its OAuth2 token
endpoint) answers within `timeoutMs`; SSO is healthy when at least one provider is. Results are
shared by concurrent callers and reused for 5 seconds. A provider that never answers counts as
unhealthy after `timeoutMs`, so a hung provider cannot hold a login request open.

## Break-glass

`auth.breakGlass: {enabled, users[]}` lists local accounts (argon2id or bcrypt hashes, same
format as `auth.local.users`) for when the identity provider is down. It applies only with
`backend: sso`. `POST /auth/login` accepts a break-glass user only while SSO is unhealthy; in every
other case, including a healthy provider, an unknown user and a wrong password, it answers the same
401 `invalid_credentials`. The password is verified in all cases (an unknown user against a dummy
hash), so timing does not reveal which case applied. Each success logs at WARN with the username
and client IP; each failure logs at INFO and never includes the password. The web login page shows
the break-glass form only when the server reports SSO unhealthy and break-glass enabled. A
break-glass session lasts until it expires, even if SSO recovers.

## Passwords: argon2id and bcrypt

Passwords are stored as argon2id PHC strings and verified with `hash-wasm`, a pure WebAssembly
build, so no native module is needed. Existing bcrypt hashes keep working and log a warning at
startup so they can be re-hashed.

## Sessions

Sessions are held in server memory with a sliding expiry and a cap on their number. A restart
signs everyone out, and in-flight SSO logins are lost too. Run one Canopy instance per
deployment, or use sticky routing, because sessions are not shared between instances.

## Out of scope

LDAP and SAML backends, several identity providers resolving to one account, audience checks on IdP
bearer tokens, and a session store that survives restarts.
