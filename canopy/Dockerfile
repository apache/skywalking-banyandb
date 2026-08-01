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

# ── deps: full workspace install for the build ─────────────────────────────
FROM node:24.6.0-bookworm AS deps
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
RUN npm ci --omit=dev

# ── runtime: distroless node24. Node >= 23.6 strips TypeScript types natively,
#    which is how the BFF resolves canopy-shared (exports raw src/index.ts —
#    see server/tsconfig.json paths). nonroot, no shell, no package manager. ──
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
