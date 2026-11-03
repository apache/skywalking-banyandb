This opt-in test imports an independently downloaded native trace segment into a
real hot data node, starts two warm data nodes and a liaison, and runs the actual
lifecycle executable as a separate process. It runs an uninterrupted migration
and a second migration interrupted after an acknowledged durable replay cursor.
The second process resumes with the sampler plugin unavailable, demonstrating
that it reuses prepared decisions. Public trace and timestamp-index queries check
retained payloads and dropped traces. Source cleanup is checked after delivery.

The fixture directory must contain `source/sw_trace/seg-20261012`, metadata API
wrappers in `metadata/{group,trace,index-rules,index-bindings}.json`, a matching
native first-party sampler in `plugins/sw-trace-sampler.so`, and the lifecycle
executable in `bin/lifecycle`. `REAL_LIFECYCLE_BIN` can override the executable.
Build the executable and plugin with the same Go toolchain and build flags.

```sh
REAL_TRACE_DIR=/tmp/lifecycle-showcase-20261013 \
  go test ./test/integration/distributed/lifecycle_real \
  -run '^TestRealLifecycleAgent$' -v -count=1 -timeout=10m
```

Each run preserves its node directories, child logs, pinned source snapshot,
prepared sampling artifacts, interruption and resumed checkpoints, and aggregate
verification report under a new `agent-e2e-*` directory inside the fixture root.
The original downloaded data is preserved. The local source TTL is one hour and
the warm TTL is 3,650 hours so the historical source is eligible without changing
its timestamps or payloads. Both stages retain the showcase's two-shard layout;
a fixture containing only shard 1 sends data to its assigned warm node.

Without `REAL_TRACE_DIR`, the test skips and requires no downloaded data or native
plugin. No real trace payloads or fixture files belong in the checkout.
