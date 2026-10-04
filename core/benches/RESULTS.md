# Initial value clone measurement

## Environment and reproduction

- Date: 2026-10-04 (Asia/Seoul).
- Base commit: `705cf9275916cf8f3513ed30c0713fea7909a3e4`.
- Working tree: benchmark addition uncommitted; modified `core/Cargo.toml` and
  `Cargo.lock`, with new files under `core/benches/`. The tree was clean before
  this addition. The results document was written after measurement.
- CPU: Apple M1 Pro, 10 cores (8 performance, 2 efficiency); 32 GB memory.
- OS: macOS 26.6.2 (25G83), Darwin 25.6.0, arm64.
- Compiler: rustc 1.94.1 (`e408947bf`, 2026-03-25), LLVM 21.1.8,
  `aarch64-apple-darwin`.
- Harness: Criterion 0.3.6, default settings (3-second warm-up, 5-second target
  measurement, 100 samples), optimized Cargo bench profile, plotters backend.
- Command: `cargo bench -p gluesql-core --bench value_clone`.
- All 27 cases completed successfully. Before measuring,
  `cargo bench -p gluesql-core --bench value_clone -- --test` passed all cases
  and their untimed assertions.

The local console log is `target/value-clone-benchmark.log`; raw estimates and
reports are under `core/target/criterion`. These generated artifacts are not
committed. Machine background activity was not controlled, and this is one run,
not a cross-machine or repeatability study.

## Timings

All cells are **nanoseconds per operation**, shown as
`estimate [95% confidence interval lower, upper]`. Values come from Criterion's
`new/estimates.json` slope estimate, or mean if a slope is unavailable. Inputs
and timing boundaries are defined in [README.md](README.md).

| Input | `value_clone_drop` | `rc_clone_drop` | `rc_new` |
| --- | --- | --- | --- |
| i64/42 | 2.560 [2.554, 2.566] | 4.100 [4.093, 4.109] | 8.037 [8.028, 8.048] |
| string/16_bytes | 20.169 [20.143, 20.203] | 4.086 [4.080, 4.094] | 8.127 [8.067, 8.238] |
| string/256_bytes | 24.975 [24.944, 25.011] | 4.128 [4.123, 4.134] | 8.042 [8.031, 8.053] |
| string/4096_bytes | 86.671 [86.149, 87.303] | 4.083 [4.080, 4.087] | 8.726 [8.701, 8.754] |
| string/65536_bytes | 1469.180 [1420.094, 1527.622] | 4.087 [4.082, 4.092] | 8.372 [8.334, 8.415] |
| map/8_entries/16_bytes_per_value | 365.278 [364.795, 365.866] | 4.127 [4.122, 4.132] | 8.169 [8.136, 8.207] |
| map/8_entries/256_bytes_per_value | 397.548 [397.018, 398.262] | 4.117 [4.087, 4.165] | 9.060 [9.021, 9.105] |
| map/128_entries/16_bytes_per_value | 5679.507 [5655.336, 5707.784] | 4.134 [4.124, 4.145] | 10.389 [9.945, 11.214] |
| map/128_entries/256_bytes_per_value | 6393.210 [6380.632, 6407.026] | 4.207 [4.139, 4.301] | 10.611 [10.346, 10.903] |

## Observations and limits

In this run, copying and destroying the integer was cheaper than cloning and
destroying an existing shared reference. Direct string copying rose from about
20 ns at 16 bytes to 1.47 microseconds at 65,536 bytes. Map copying ranged from
about 365 ns to 6.39 microseconds. Existing shared-reference clone-and-drop
timings stayed around 4.1 ns across these inputs.

New shared allocation measured approximately 8–11 ns with the input already
prepared. Those timings include value movement and batch bookkeeping, exclude
input preparation and final destruction, and reflect allocator/cache state
affected by untimed work. They must not be added to clone-and-drop timings to
claim an exact break-even point.

Criterion reported outliers; the largest string's direct copying and some Map
cases showed wider intervals. The small-operation results also include harness
and optimization-barrier overhead. Confidence intervals describe this run's
statistical estimates, not variation across machines or repeated sessions.

The measurements support investigating expensive string and Map copies while
preserving a scalar comparison. They do not establish that wrapping every row
value in `Rc` would improve query performance. Row memory/layout, ownership-based
clone avoidance, and realistic query measurements remain follow-up work.
