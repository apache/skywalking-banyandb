# Build environment

`scripts/build/dockerfiles/build.Dockerfile` builds the pinned environment used to produce the
artifacts this repository commits — currently the generated license files. The design rationale is
in [`docs/design/0.12.0/docker-canonical-build/README.md`](../../docs/design/0.12.0/docker-canonical-build/README.md).

```shell
make docker-license-dep     # regenerate the license files, canonically
make docker-run TARGET=<t>  # run another target that does not need Git
make bump-build-image        # refresh the Debian digest and snapshot in version.mk
```

The image is not published. It is built locally and cached in `bin/.buildkit`.

## What is pinned, and where

| Input | Declared in | Derived or stored |
| --- | --- | --- |
| Go version | `go.mod` | derived — see below |
| Node version | `mcp/package.json` and `canopy/package.json` `engines.node` | both must pin it exactly; read directly |
| license-eye | `version.mk` `LICENSE_EYE_VERSION` | stored |
| Build platform | `version.mk` `BUILD_PLATFORM` | stored, not overridable |
| Debian base image | `version.mk` `DEBIAN_IMAGE` | digest |
| Debian package index | `version.mk` `DEBIAN_SNAPSHOT` | snapshot date |
| Go / Node tarballs | the vendor's published checksum, fetched at build time | nothing to maintain |
| Resource limits | `dockerize.mk` defaults | overridable per run |

`scripts/build/version.mk` is the single place the *pins* are written down, and the two subprojects'
`package.json` files are the source for the *toolchain versions*. `dockerize.mk` and the Dockerfile
contain no version literals, so a bump is one edit in one place.

Neither the Go nor the Node version is in `version.mk`, on purpose. The `go` directive in `go.mod` and
the `engines.node` fields in `mcp/package.json` and `canopy/package.json` are what the toolchains
themselves enforce, so declaring either twice would create two sources that can silently disagree.
Both are derived and passed to the Dockerfile as build args, and the entrypoint fails the build if the
image and either declaration drift apart.

There is no generated version file. `actions/setup-node` reads `engines.node` straight out of a
`package.json` given as `node-version-file`, so the workflows point at `canopy/package.json` and there
is nothing to keep in step. `make check-node-version` fails if mcp and canopy do not pin the same
exact version, or if any other project is not satisfied by that pin.

The Go and Node tarballs are verified against the checksum each vendor publishes for that exact file
(`https://go.dev/dl/?mode=json` and `https://nodejs.org/dist/v<VERSION>/SHASUMS256.txt`), fetched over
TLS during the build, so no checksum has to be refreshed by hand. A version with no published checksum
fails the build rather than installing unverified bytes. The trade-off is that this catches corruption
and truncation, not a compromised origin — the same trust model the rest of the build already accepts,
since `go install` verifies license-eye through the module proxy and license-eye then runs `npm ci`
against the npm registry. A base-image digest and an apt snapshot cannot be derived from a URL, which
is why those two are recorded rather than resolved.

The build fails if the base image or the snapshot is missing from `version.mk`, rather than silently
building something unpinned.

The build fails if the base image or snapshot is missing from `version.mk`, rather than silently
building something unpinned.

## Bumping Go or Node

1. Node: edit `engines.node` in **both** `mcp/package.json` and `canopy/package.json` to the same
   exact version. Go: edit `go.mod`. Regenerate the `package-lock.json` files — npm records the root
   `engines` there too, so a stale lockfile is a real (and confusing) failure mode. Nothing else
   needs to change; CI reads the manifests directly.
2. `make check-node-version` — fails if mcp and canopy disagree, or if any other project's
   `engines.node` is not satisfied by the pin.
3. `make bump-build-image` is only needed to move the Debian base image or the apt snapshot; it
   rewrites `version.mk` through a temp file, so a failed lookup leaves the previous pins intact.
4. `make license-dep` **natively** — this must stay green. If the bump changes resolved license
   text, that diff belongs in the same PR, produced by a path that does not depend on the image.
5. `make docker-license-dep` and confirm `make license-manifest` is unchanged. Identical output to
   step 4 is the first evidence the image is correct.
6. Push. The ubuntu `check` job compares the native and container manifests and verifies the
   committed bytes.

Step 4 is why CI runs both the native and the container path and compares them: a broken image must
not be able to hide a license change, and a license change must not be able to hide a broken image.

The build system is checked on Linux, macOS and Windows by
`.github/workflows/test-build-system.yml`: the Node resolver, the verifier in its passing and failing
modes, and one container run that builds the pinned image and confirms it carries the versions this
repository declares. Generation itself is only run on ubuntu, where the check job already does it --
once `eol=lf` normalizes the checkout and the tree is streamed into the container rather than
mounted, the host is not a variable in the output.

Host requirements for `make docker-license-dep` are deliberately minimal: `docker`, GNU `make`, and
a `tar` with `--exclude` (GNU tar, or the bsdtar built into Windows 10+). A POSIX `id` is used when
present and not required when absent, so a native Windows shell with Docker Desktop works; WSL2 is
easier because it also supplies GNU make and GNU tar. Without bash, the post-generation
`check-license-outputs` is skipped with a notice rather than failing the build — CI runs it
unconditionally.

## Resource limits

Defaults are 2 CPUs and 4 GB, matching `make test-docker`. Override per run, e.g.
`make docker-license-dep RUN_CPUS=4 RUN_MEMORY=8g`. The container also gets `GOMAXPROCS` and a V8
heap cap matched to the CPU limit, because a Go or Node process sizing itself from host resources
makes timing host-dependent.

If a cold-cache run OOMs, raise the limits — do not remove them.

## Entrypoint

`entrypoint.sh` unpacks the tree streamed in on stdin, cross-checks the image's Go and Node against
`go.mod` and the mcp and canopy `package.json` files, runs the requested make target, and copies only the license artifacts
into `/out`. It never invokes Git: a linked worktree keeps its `.git` in a file pointing outside the
tree, which is one of several reasons the worktree is copied rather than bind-mounted.
