# Value clone benchmarks

These microbenchmarks support the performance investigation in
[#1864](https://github.com/gluesql/gluesql/issues/1864). They compare copying a
`Value`, cloning an existing `Rc<Value>`, and allocating a new `Rc<Value>` in the
current synchronous execution architecture. They do not change the engine or
measure query performance.

## Inputs

Each of the following nine inputs is measured with all three operations, giving
27 benchmark cases:

| Value | Input sizes |
| --- | --- |
| `Value::I64` | `42` |
| `Value::Str` | 16, 256, 4,096, and 65,536 ASCII bytes |
| `Value::Map` | 8 or 128 entries, each containing a 16- or 256-byte ASCII string |

Maps use `BTreeMap<String, Value>` and eight-byte keys such as `key_0000`.
Entry counts and string lengths describe the logical input, not allocated heap
memory. Input shape, clone equality, shared pointer identity, and reference count
restoration are checked before timing. Input and output values pass through
`std::hint::black_box` to discourage optimization of the measured work.

## Timing boundaries

| Operation | Included | Excluded |
| --- | --- | --- |
| `value_clone_drop` | `Value::clone()` and destruction of the copy | Original input preparation and destruction |
| `rc_clone_drop` | `Rc::clone()` and destruction of the new reference | Original `Rc` allocation and final destruction |
| `rc_new` | `Rc::new(value)`, moving the prepared value, and batch timing overhead | Input cloning and destruction of the resulting `Rc` and its value |

The original shared reference remains alive throughout `rc_clone_drop`, so its
timed destruction decrements the reference count without freeing the value.
The clone-and-drop cases use Criterion's `iter` with explicit destruction inside
the timed closure. `rc_new` uses `iter_batched`: setup clones the input outside
timing, and the routine returns the newly allocated `Rc` for destruction after
timing. Batches contain at most 64 iterations (`BatchSize::NumIterations(64)`),
limiting retained input and output memory. Preparing inputs still affects the
allocator and cache state; this is not a measurement of allocator calls in
isolation. Batch bookkeeping and output collection overhead must not be
interpreted as allocation cost alone.

## Reproduction

Run from the repository root:

```sh
cargo bench -p gluesql-core --bench value_clone -- --test
cargo bench -p gluesql-core --bench value_clone
```

The first command runs all cases once, including the untimed assertions. The
second uses Criterion's default warm-up, sample count, and measurement settings.
Criterion writes reports under `core/target/criterion` and prints timing estimates
with confidence intervals. Run on an otherwise idle machine and retain the
output alongside the environment information:

```sh
git rev-parse HEAD
git status --short
rustc -Vv
uname -a
# macOS CPU and OS details:
system_profiler SPHardwareDataType
sw_vers
```

Record the CPU model, operating system, compiler, commit, working tree changes,
exact command, and input sizes with any published results. Compare timing
estimates and confidence intervals by operation and input size; timings are
specific to the measured environment, not performance thresholds.

## Interpretation and follow-up

Large strings and maps may make direct copying more expensive than sharing a
reference. Small scalars may favor direct copying. These benchmarks measure
clone-and-drop lifecycles and separately measure initial shared allocation;
neither observation demonstrates an engine-level speedup. Do not add these
timings to derive an exact break-even point, or treat logical input sizes as a
row memory measurement.

See [the initial measurement](RESULTS.md) for an environment-specific example
covering all 27 cases and their confidence intervals.

Follow-up investigations should measure row memory and layout, examine whether
evaluator/context borrowing can avoid copies, and validate realistic query
performance before deciding whether to store `Rc<Value>` in rows. This benchmark
contribution alone does not resolve #1864. No automated CI performance threshold
is introduced.
