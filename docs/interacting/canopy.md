# Web Console (Canopy)

Canopy is the official web console of BanyanDB. It is a standalone web application — a React single-page app served by a Node.js BFF — that connects to the BanyanDB HTTP API (the gRPC-gateway on port `17913`) and lets you browse schemas, query measures, streams, traces and properties from a browser.

Canopy ships as a separate Docker image, `apache/skywalking-banyandb:<version>-canopy`, and runs as its own process alongside a BanyanDB server. BanyanDB itself exposes only the client API under `/api` on port `17913`. Canopy is also published as a tarball, `skywalking-banyandb-<version>-canopy.tgz` — see [Get Binaries](../installation/binaries.md) for the download location and the unpack-and-run steps.

## Run with Docker

Canopy needs a BanyanDB server it can reach, a session secret and a users file. Create a `users.yaml` listing the console users (see [Generating a password hash](#generating-a-password-hash)):

```yaml
- username: admin
  passwordHash: <bcrypt hash>
  role: admin    # admin (read-write) | readonly
```

`BANYANDB_TARGET` must be reachable from inside the Canopy container. When BanyanDB runs in another container, put both on the same Docker network and point at its container name (a bare `localhost` would refer to the Canopy container itself):

```shell
docker network create banyandb-net
docker network connect banyandb-net banyandb   # your BanyanDB container

docker run -d \
  --network banyandb-net \
  -p 4000:4000 \
  -e BANYANDB_TARGET=http://banyandb:17913 \
  -e SESSION_SECRET=<random secret of at least 32 characters> \
  -e CANOPY_USERS=/etc/canopy/users.yaml \
  -v $(pwd)/users.yaml:/etc/canopy/users.yaml:ro \
  --name canopy \
  apache/skywalking-banyandb:<version>-canopy
```

Replace `<version>` with the release you run, for example `0.11.1-canopy`.

Open http://localhost:4000 and log in with one of the users defined in the file.

If the BanyanDB server enforces Basic-Auth on its HTTP API, configure the upstream credential so Canopy attaches it to every proxied API request:

```shell
-e BANYANDB_UPSTREAM_USERNAME=<username> \
-e BANYANDB_UPSTREAM_PASSWORD=<password>
```

## Generating a password hash

`passwordHash` is a bcrypt hash of the user's password (not the password itself). Generate one with the Canopy image — the trailing `-e` is Node's eval flag, since the image entrypoint is `node`:

```shell
docker run --rm apache/skywalking-banyandb:<version>-canopy \
  -e "console.log(require('bcryptjs').hashSync('your-password', 10))"
```

## Run from source

The [Canopy README](https://github.com/apache/skywalking-banyandb/tree/main/canopy) covers running from a checkout in two shapes: the two-terminal dev setup (SPA dev server on `5173`, BFF on `4000`), and a production build where the BFF alone serves the built SPA on `4000`. Its [`.env.example`](https://github.com/apache/skywalking-banyandb/blob/main/canopy/.env.example) documents every setting: session secret, user file, reverse-proxy base path and metrics monitoring target.
