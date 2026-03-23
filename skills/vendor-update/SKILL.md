---
name: vendor-update
description: >
  Upgrade Go/Node.js vendor dependencies and sync tool versions. Use whenever the
  user says "upgrade dependencies", "update vendors", "vendor update", "run vendor-upgrade",
  "bump dependencies", "update packages", or asks to run the `vendor-update` Make target.
  This skill also checks `scripts/build/version.mk` after upgrading to see if any
  tracked tool versions need updating too, and removes stale binaries from `bin/`
  when versions change.
compat:
  - go
  - make
---

## Upgrade vendor dependencies

Run the vendor-update Make target across all projects:

```bash
make vendor-update
```

This runs `go get -u ./...` and `go mod tidy -compat=1.25` in each project directory (ui, banyand, bydbctl, mcp, fodc/agent, fodc/proxy, pkg, test).

## Check for tool version drift

After upgrading, compare `go.mod` dependency versions against `scripts/build/version.mk` to find any tools whose library version was bumped and may need their tool version updated.

### Version mapping

Read `scripts/build/version.mk` and `go.mod` in parallel. For each tool version variable, check if the corresponding library in go.mod was upgraded:

| version.mk variable | Binary | go.mod package |
|---|---|---|
| `BUF_VERSION` | `bin/buf` | `github.com/bufbuild/buf` (if present) |
| `PROTOC_GEN_GO_VERSION` | `bin/protoc-gen-go` | `google.golang.org/protobuf` |
| `PROTOC_GEN_GO_GRPC_VERSION` | `bin/protoc-gen-go-grpc` | `google.golang.org/grpc` |
| `PROTOC_GEN_DOC_VERSION` | `bin/protoc-gen-doc` | `github.com/pseudomuto/protoc-gen-doc` (if present) |
| `GRPC_GATEWAY_VERSION` | `bin/protoc-gen-grpc-gateway` | `github.com/grpc-ecosystem/grpc-gateway/v2` |
| `PROTOC_GEN_VALIDATE_VERSION` | `bin/protoc-gen-validate` | `github.com/envoyproxy/protoc-gen-validate` |
| `GOLANGCI_LINT_VERSION` | `bin/golangci-lint` | (no go.mod entry — only in version.mk) |
| `REVIVE_VERSION` | `bin/revive` | (no go.mod entry — only in version.mk) |
| `LICENSE_EYE_VERSION` | `bin/license-eye` | (no go.mod entry — only in version.mk) |
| `MOCKGEN_VERSION` | `bin/mockgen` | `go.uber.org/mock` |
| `GINKGO_VERSION` | `bin/ginkgo` | `github.com/onsi/ginkgo/v2` |
| `GOVULNCHECK_VERSION` | `bin/govulncheck` | `golang.org/x/vuln` |
| `BPF2GO_VERSION` | (used in bpf2go.mk) | `github.com/cilium/ebpf` |

**Only update version.mk if the library version in go.mod has changed and the tool version should follow.** Many tools (golangci-lint, revive, license-eye) are not in go.mod — their versions only change when explicitly updated in version.mk, not from `make vendor-update`.

## Update version.mk if needed

If a tool version should be bumped (library changed and tool version needs to match), update `scripts/build/version.mk` using the Edit tool. Keep the format exactly — e.g., `PROTOC_GEN_GO_VERSION := v1.36.11`.

## Remove stale binaries

When version.mk is updated, remove the corresponding old binary from `bin/`:

```bash
rm bin/<binary-name>
```

This ensures the next build/install of that tool fetches the correct new version.
