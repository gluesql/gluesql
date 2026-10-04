# Initial evaluation and query measurements

## Environment

- Date: 2026-10-04 (Asia/Seoul).
- Base commit: `42a4b4189b06c57ddf40f24a5e47829966d80db0`.
- Working tree at measurement: uncommitted additions of these benchmark targets
  and methodology/ownership documentation; modified `Cargo.lock`,
  `core/Cargo.toml`, and `storages/memory-storage/Cargo.toml`. The branch was
  created from clean upstream main. This results document was generated after
  both measurements completed.
- CPU: Apple M1 Pro, 10 cores (8 performance and 2 efficiency); 32 GB memory.
- OS: macOS 26.6.2 (25G83), arm64.
- Compiler: rustc 1.94.1 (`e408947bf`, 2026-03-25), LLVM 21.1.8,
  `aarch64-apple-darwin`.
- Harness: Criterion 0.3.6, default settings (3-second warm-up, 5-second target
  measurement, 100 samples), optimized Cargo bench profile, plotters backend.
- Validation: all-target Clippy, formatting, and both test-mode benchmark runs
  passed, including all 40 cases and the untimed correctness assertions.

The targets ran sequentially using:

```sh
cargo bench -p gluesql-core --bench evaluator
cargo bench -p gluesql_memory_storage --bench query_evaluation
```

Background machine activity was not controlled. These are single-run,
endpoint-specific baselines, not a before/after optimization comparison or a
repeatability study. [EVALUATION.md](EVALUATION.md) defines inputs, included
costs, exclusions, and the ownership investigation.

## Evaluator results

Units are **nanoseconds per call**. Each cell is
`estimate [95% confidence interval lower, upper]`, using Criterion's slope
estimate or mean when a slope is unavailable.

| Input / identifier | Evaluate and drop | Convert to Value and drop | Equality and drop |
| --- | --- | --- | --- |
| i64 / plain | 32.547 [32.507, 32.589] | 34.323 [34.272, 34.384] | 58.121 [58.031, 58.236] |
| i64 / qualified | 34.477 [34.424, 34.531] | 36.275 [36.188, 36.369] | 62.269 [62.142, 62.417] |
| string_256 / plain | 61.395 [60.423, 63.176] | 61.951 [61.856, 62.056] | 116.814 [116.514, 117.211] |
| string_256 / qualified | 61.448 [61.324, 61.574] | 62.451 [62.342, 62.573] | 119.127 [119.003, 119.293] |
| string_4096 / plain | 124.504 [124.005, 125.097] | 126.255 [125.844, 126.684] | 349.452 [348.416, 350.714] |
| string_4096 / qualified | 125.622 [125.036, 126.266] | 126.034 [125.500, 126.630] | 352.560 [351.740, 353.386] |
| map_8x256 / plain | 437.503 [433.948, 442.788] | 441.471 [436.008, 451.516] | 1404.307 [1403.167, 1405.634] |
| map_8x256 / qualified | 433.335 [432.892, 433.844] | 435.434 [434.959, 435.973] | 1410.681 [1409.156, 1412.382] |

## Query results

Units are **microseconds per complete query over 1,000 rows**, including owned
result destruction. Each cell uses the same estimate/95% interval convention.
The integer filter returns 500 rows; the other queries return 1,000 rows.

| Payload | Integer filter | Projection | Repeated projection | Payload equality |
| --- | --- | --- | --- | --- |
| i64 | 151.310 [151.125, 151.524] | 140.422 [140.163, 140.826] | 158.896 [158.747, 159.077] | 191.342 [190.933, 191.789] |
| string_256 | 170.248 [169.993, 170.575] | 197.572 [197.295, 197.877] | 249.399 [249.011, 249.881] | 281.080 [280.338, 281.950] |
| string_4096 | 290.057 [289.553, 290.682] | 446.821 [445.778, 447.953] | 763.865 [761.029, 766.980] | 681.278 [679.982, 682.828] |
| map_8x256 | 614.345 [612.708, 616.649] | 1067.153 [1064.919, 1069.434] | 1640.493 [1634.787, 1647.096] | 1640.515 [1634.876, 1645.957] |

## Observations

- Plain-identifier evaluation-and-drop measured about 32.5 ns for the integer,
  61.4 ns for a 256-byte string, 125 ns for a 4,096-byte string, and 438 ns for
  the Map. These calls include context construction, internal Rc allocation,
  lookup, cloning, and destruction; they are not isolated copy costs.
- Conversion to owned Value has similar timings in these cases. The current
  evaluator already returns an owned column value, so conversion moves it
  rather than making another deep copy.
- Equality includes two operand evaluations and comparison work. Plain Map
  equality measured about 1.40 microseconds; attributing that entire time to
  cloning would be incorrect.
- Integer-filter queries increased from about 151 microseconds with integer
  payloads to 290 microseconds with 4 KiB strings and 614 microseconds with
  Maps, despite returning only IDs. MemoryStorage copies payloads during its
  scan, so these are not isolated scalar-evaluation measurements.
- Repeated projection measured about 764 microseconds for 4 KiB strings and
  1.64 milliseconds for Maps. It produces two owned payload columns per row.
  Map payload equality also measured about 1.64 milliseconds, but returns IDs
  and includes comparisons; similar times do not imply identical costs.

## Dispersion and retained artifacts

Criterion reported outliers. We screened for a 95% interval width greater than
10% of the point estimate as a practical trigger for an additional run; none
of the 40 cases met it, and no case failed. Consequently no measurement reruns
were performed. This screening is not a performance threshold or proof of
cross-run stability. Intervals describe the estimates within this run, not
variation across machines or sessions.

Some slower query cases required more than the five-second target to collect
100 samples. Criterion extended collection automatically; for example, Map
payload equality collected 100 samples in an estimated 8.2342 seconds. Default
settings were retained rather than reducing the sample count.

Local generated artifacts, excluded from the commit:

- `target/evaluator-benchmark.log`
- `target/query-evaluation-benchmark.log`
- `target/evaluator-initial-estimates.json`
- `target/query-evaluation-initial-estimates.json`
- `target/evaluation-wide-cases.json`
- `core/target/criterion` and `storages/memory-storage/target/criterion`

## Implications for the next PR

These baselines provide comparison points for a future borrowing prototype;
no speedup has been demonstrated. Source inspection identifies condition
operands consumed before context destruction as the first candidate. Stateless
column returns and projections still require owned data at their return
boundaries. A prototype must preserve expression semantics, NULL/error
behavior, and temporary-context lifetime safety before it can be evaluated.

Use both targets when comparing a future change, and retain the storage scan
and result construction limitations. Row memory/layout and Rc-backed row
allocation/final destruction have not been measured in this PR.
