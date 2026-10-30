# QG3 validation

All commands ran from the QG3 worktree using `source /tmp/pr4-env.sh` (Go 1.26.10). Generated protobufs and mocks were copied from the QG2 worktree as ignored build artifacts; no protobuf source was changed.

- `go test ./pkg/bydbql ./pkg/query/ql ./pkg/decimal -count=1` passed, as did the focused vectorized, measure serializer and frame suites. Decimal arithmetic was checked against a `math/big` oracle including the 38-digit boundary. Frame tests reject decimal transport.
- `go test ./test/cases/measure/qlgen -count=1` passed; the generator consistency check also passed after the last case additions.
- `go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query -ginkgo.label-filter='measure-ql && (QLB-T14 || QLB-T15 || QLB-T16 || QLB-T17)' -ginkgo.fail-on-empty` passed.
- The corresponding distributed command ran 299 specs: 289 passed and 10 failed. The seven T15, one T16 and two T17 failures are the previously reproduced baseline memory pool failures; no unrelated fix was attempted. Full output: [qg3-compat-distributed.log](validation-logs/qg3-compat-distributed.log).
- `golangci-lint run ./pkg/bydbql ./pkg/decimal ./pkg/query/ql ./pkg/query/vectorized/... ./test/cases/measure/qlgen ./verification/qg3bench` passed with zero issues.
- PostgreSQL 17.6 oracle SQL and actual output are adjacent to this file. The precision-38 overflow boundary is tested at the arithmetic interface: int64 inputs and the int64 row-count limit cannot feasibly reach it through normal ingestion.

The final QG3 generated RPC suite results are recorded below after the last implementation and fixture changes. Benchmark methodology and results are in [README.md](README.md).

Final generated RPC verification passed in both standalone and distributed modes after the last changes:

```sh
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/standalone/query -ginkgo.label-filter='measure-ql && QLB-T07' -ginkgo.fail-on-empty
go test -tags=querymetrics -count=1 -timeout=30m ./test/integration/distributed/query -ginkgo.label-filter='measure-ql && QLB-T07' -ginkgo.fail-on-empty
```

Successful command output is in [qg3-standalone-final2.log](validation-logs/qg3-standalone-final2.log) and [qg3-distributed-final2.log](validation-logs/qg3-distributed-final2.log); final lint output is [qg3-scoped-lint-passed.log](validation-logs/qg3-scoped-lint-passed.log).
