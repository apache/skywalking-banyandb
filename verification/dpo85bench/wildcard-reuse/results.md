# Native wildcard comparison results

Three fixed fresh-process samples per arm and revision, 100 queries per sample; medians below.
Every query returned the expected row identities and released its allocation reservations.

| Arm | Before µs/op | After µs/op | Latency change | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| prefix | 376.028 | 38.448 | -89.8% | 130068 | 19875 | 465 | 319 |
| leading | 1668.145 | 705.705 | -57.7% | 186424 | 19004 | 576 | 315 |
| no_match | 1922.068 | 702.785 | -63.4% | 192265 | 7640 | 345 | 58 |

| Arm | Before latency range µs/op | After latency range µs/op | Before reserved B/op | After reserved B/op |
| --- | ---: | ---: | ---: | ---: |
| prefix | 374.712–385.127 | 37.821–40.321 | 16612 | 26204 |
| leading | 1661.315–1690.984 | 683.105–742.310 | 16612 | 24780 |
| no_match | 1913.198–1989.051 | 659.649–718.288 | 0 | 5372 |

Negative latency change means faster. These three samples describe this fixed native workload;
they are not a statistical regression gate or an end-to-end QL latency claim.

Reserved bytes are a conservative query accounting allowance, not physical allocation or RSS.
The old compiler did not reserve its wildcard allocations; the shared engine does.

[Method and reproduction](README.md) · [All samples](samples.json) · [Summary](summary.json) ·
[Provenance](provenance.json) · [Source hashes](source-hashes.json)
