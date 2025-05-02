# Building fadvise-bpf Benchmark Environment

This document describes how to build the eBPF-based benchmarking system for the `fadvise` subsystem using `bpf2go`, `clang`, and `make`, with full support for multi-platform Docker environments (x86_64, arm64). This replaces older scripts with a modern, platform-aware toolchain.

## 🛠 Prerequisites

Before building, ensure you have the following installed:

- Go 1.22+ (Go 1.24 recommended)
- `clang`, `llvm`, `libelf-dev`, `bpftool`, `linux-headers-$(uname -r)`
- `make`
- (Optional) `docker`, `docker buildx` for container-based builds

You can install all dependencies in a clean environment using Docker (see below).

---

## ✅ Local Build with Makefile (Recommended)

The project uses a unified `Makefile` in `test/stress/fadvis/` that builds both x86 and arm64 targets, generates `vmlinux.h`, and runs benchmarks.

### Generate BPF Loaders

```bash
cd test/stress/fadvis
make bpf
```

This will:
- Generate `vmlinux.h` using `bpftool`
- Compile `bpfsrc/fadvise.c` via `bpf2go` into platform-specific `.o` and `.go` files:
  - `bpf/bpf_x86_bpfel.go` (for amd64)
  - `bpf/bpf_arm64_bpfel.go` (for arm64)

You can clean all generated files:

```bash
make clean
```

---

## 🐳 Docker-Based Build (Cross-Platform / CI / MacOS)

You can also build and benchmark entirely within Docker using the included Dockerfile.

### Build Docker Image

```bash
docker buildx build \
  --platform linux/amd64 \
  -t fadvis-bpf:amd64 .

# or for ARM64:
docker buildx build \
  --platform linux/arm64 \
  -t fadvis-bpf:arm64 .
```

### Run Benchmark in Container

```bash
docker run --rm --privileged fadvis-bpf:amd64
```

This will:
- Install required packages
- Run `make bpf` inside the container
- Run default Go benchmark test with BPF enabled

---

## 📁 Output File Structure (after `make bpf`)

```
bpf/
├── bpf_arm64_bpfel.go      // Build tag: arm64 && linux
├── bpf_arm64_bpfel.o
├── bpf_x86_bpfel.go        // Build tag: (386 || amd64) && linux
├── bpf_x86_bpfel.o
├── vmlinux.h               // Auto-generated from host kernel
```

---

Next section: [Benchmarking with eBPF](#) (coming soon)

