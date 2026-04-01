# Changes by Version

Release Notes.

## 0.10.1

### Bug Fixes

- Fix reuse of byte arrays in min/max implementation causing data corruption.
- Fix flaky trace query filtering due to non-deterministic sidx tag ordering.
- Fix index-mode measure queries returning documents outside requested time range.
- Fix nil pointer panic in segment collectMetrics during shutdown.
- Fix entity tag handling in trace filter preventing TagIdx index mismatch.
- Fix unstable tag filter matching order in sidx.
- Fix property schema client connection instability after data node restart.
- Fix duplicate TopN query execution in distributed measure queries.
- Fix bydbctl validation for malformed YAML input.
- Fix FODC agent test instability in Basic Metrics Buffering test.