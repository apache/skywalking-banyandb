# LIKE/ILIKE benchmark results

All three predetermined rounds completed on 2026-10-28. Each round used a fresh
process, two pinned CPUs, 4 GiB memory without additional swap, and an explicit
3 GiB native query allowance. Every timed count was verified. The table reports
medians and ranges of the three 100-operation means, not per-request percentiles.
The [54 individual samples](benchmark-samples.json) and [campaign provenance](benchmark-provenance.json)
are included with this report. Verbose logs and the candidate source/binary snapshot
remain local in the ignored `logs/` directory. This campaign predates the later
review fixes for ambiguous index rules, contextual keywords, shared metadata lookup,
transport-codec placement and shared limits. The numbers are historical measurements
of the archived candidate, not measurements of the corrected source. No additional
timing campaign was run for these fixes. The original measured harness is retained
in the local source archive.

Leading-percent LIKE and ILIKE take about 4.6–4.8 ms over 1,000 names and
34.5–34.6 ms over 10,000 names here. Equivalent IN lookups take about 0.5–0.6 ms.
The pattern operation discovers matching dictionary terms; the IN baseline is
given those terms and excludes discovery. Both select exactly eight names and
read sixteen raw rows. No-match patterns read zero raw rows yet take similar
time, demonstrating that dictionary exploration dominates these cases.

The allocation cost is also material: approximately 1.08 MB and 19,900 allocations
per matching operation at 1,000 names, increasing to 8.10 MB and 167,000
allocations at 10,000. Peak tracked query memory is 2.66 and 11.44 MiB. This is
bounded indexed search, with a measured cost for leading wildcards; it is not a
claim that substring discovery is as cheap as exact lookup.

The isolated unchanged exact-query control has mixed results. At 1,000 names,
median candidate latency is 0.462 ms versus 0.414 ms baseline, and all three
candidate round means exceed the baseline range. At 10,000 names it is
0.358 ms versus 0.386 ms, with overlapping ranges. CPU and allocation changes
are much smaller. These three rounds do not establish the cause of the small
control shift or a uniform exact-query regression/improvement. No percentage
pass gate or extra repeat-until-pass samples were applied.

| Source | Names | Arm | Median ms (range) | CPU ms | Bytes/op | Allocs/op | Native rows | Peak tracked MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| baseline-exact | 1000 | exact_control | 0.414 (0.382–0.429) | 0.629 | 97661 | 701 | 2 | 1.18 |
| baseline-exact | 10000 | exact_control | 0.386 (0.356–0.484) | 0.628 | 105713 | 821 | 2 | 1.18 |
| candidate-exact | 1000 | exact_control | 0.462 (0.445–0.495) | 0.647 | 98137 | 707 | 2 | 1.17 |
| candidate-exact | 10000 | exact_control | 0.358 (0.335–0.389) | 0.593 | 105642 | 821 | 2 | 1.18 |
| candidate-features | 1000 | ILIKE | 4.840 (4.505–5.107) | 6.209 | 1083305 | 19872 | 16 | 2.66 |
| candidate-features | 1000 | ILIKE_no_match | 4.442 (4.292–5.119) | 5.697 | 991360 | 19361 | 0 | 1.04 |
| candidate-features | 1000 | IN | 0.592 (0.536–0.636) | 0.797 | 127997 | 1250 | 16 | 1.63 |
| candidate-features | 1000 | IN_no_match | 0.342 (0.319–0.350) | 0.483 | 35261 | 554 | 0 | 0.01 |
| candidate-features | 1000 | LIKE | 4.643 (4.544–5.020) | 6.037 | 1079977 | 19870 | 16 | 2.66 |
| candidate-features | 1000 | LIKE_no_match | 4.446 (4.251–5.401) | 5.651 | 991847 | 19362 | 0 | 1.04 |
| candidate-features | 1000 | exact_control | 0.453 (0.383–0.512) | 0.617 | 97248 | 700 | 2 | 1.18 |
| candidate-features | 10000 | ILIKE | 34.535 (34.524–35.751) | 37.550 | 8103426 | 167327 | 16 | 11.44 |
| candidate-features | 10000 | ILIKE_no_match | 34.957 (34.069–35.117) | 37.998 | 8007287 | 167039 | 0 | 9.83 |
| candidate-features | 10000 | IN | 0.505 (0.476–0.562) | 0.792 | 143241 | 1428 | 16 | 1.63 |
| candidate-features | 10000 | IN_no_match | 0.266 (0.257–0.280) | 0.510 | 54273 | 702 | 0 | 0.01 |
| candidate-features | 10000 | LIKE | 34.648 (34.581–34.649) | 37.735 | 8102702 | 167556 | 16 | 11.44 |
| candidate-features | 10000 | LIKE_no_match | 34.317 (34.155–35.144) | 37.323 | 8007697 | 166815 | 0 | 9.83 |
| candidate-features | 10000 | exact_control | 0.329 (0.315–0.343) | 0.587 | 115775 | 848 | 2 | 1.18 |
