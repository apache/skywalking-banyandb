# Licensed to Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Apache Software Foundation (ASF) licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# canopy web console image — React SPA (web/) + Fastify BFF (server/).
# Build context: canopy/ — invoked as `make -C canopy docker` (see canopy/Makefile
# and scripts/build/docker.mk). Runtime is distroless for minimal CVE surface.
# All build stages run on BUILDPLATFORM: the web bundle is static and every
# prod dep is pure JS, so nothing needs a target-arch toolchain — this keeps
# the arm64 leg from tripping over x64-pinned dev deps (EBADPLATFORM).

# ── deps: full workspace install for the build ─────────────────────────────
FROM --platform=$BUILDPLATFORM node:24.6.0-bookworm AS deps
WORKDIR /app
COPY package.json package-lock.json ./
COPY shared/package.json shared/
COPY web/package.json web/
COPY server/package.json server/
RUN npm ci

# ── build: shared type check + web bundle + server tsc ─────────────────────
FROM deps AS build
COPY tsconfig.base.json ./
COPY shared/ shared/
COPY web/ web/
COPY server/ server/
RUN npm run build

# ── prod-deps: runtime-only node_modules (workspace symlink to shared kept) ──
FROM deps AS prod-deps
# --omit=optional as well as --omit=dev, and the reason is specific.
#
# optionalDependencies are NOT dev-scoped: npm installs them in a production
# install. The rolldown native bindings live in web's optionalDependencies
# because that is the only place npm will resolve a host-appropriate binary
# without failing the install on a platform mismatch (see skywalking#14094) --
# but they are BUILD-ONLY. Installing one per platform into the runtime
# distribution would enlarge the shipped artifact and the native attack surface
# for a tool the server never loads.
#
# Nothing at runtime needs an optional dependency: canopy/server and
# canopy/shared declare none, and neither pulls a native binding. So omitting
# optional is safe today. `npm run check:prod-deps` (run in canopy.yml) fails if
# a runtime workspace ever gains an optional dependency, so this cannot silently
# become a landmine.
RUN npm ci --omit=dev --omit=optional

# ── runtime: distroless node24. The compiled server has NO runtime imports of
#    canopy-shared (verified: `grep -r canopy-shared server/dist/src` is empty
#    — every shared import is type-only and elided by tsc), so the enums in
#    shared/src/schema.ts are never executed here. shared/ is still copied so
#    the node_modules workspace symlink resolves; if a VALUE import of
#    canopy-shared is ever added, shared must gain a real JS build — Node's
#    type stripping does not execute `export enum`, and the integration smoke
#    (boot + all endpoints) is the guardrail that would catch it. ──
FROM gcr.io/distroless/nodejs24-debian12:nonroot
WORKDIR /app
COPY --from=build /app/web/dist web/dist
COPY --from=build /app/server/dist server/dist
COPY --from=build /app/server/package.json server/package.json
COPY --from=prod-deps /app/node_modules node_modules
COPY shared/package.json shared/package.json
COPY shared/src shared/src
# The BFF serves the SPA from ../web/dist relative to its cwd (server/src/
# plugins/static.ts), so the workdir must be the server package dir.
WORKDIR /app/server
ENV NODE_ENV=production \
    PORT=4000 \
    LOG_LEVEL=info
EXPOSE 4000
# No curl/wget in distroless — healthcheck through node itself.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
  CMD ["/nodejs/bin/node", "-e", "fetch('http://127.0.0.1:4000/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]
CMD ["dist/src/index.js"]
