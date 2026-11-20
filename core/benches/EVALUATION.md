# Evaluation and query baselines

These benchmarks continue the investigation in
[#1864](https://github.com/gluesql/gluesql/issues/1864) after the value-copy
microbenchmarks in [#2049](https://github.com/gluesql/gluesql/pull/2049). They
measure current execution paths and document borrowing candidates. They do not
modify the evaluator, row representation, or public interfaces.

## Inputs and timing boundaries

Both targets use four payloads: `I64(42)`, 256- and 4,096-byte ASCII strings, and
a Map with eight entries, eight-byte `key_0000`-style keys, and 256-byte ASCII
string values. These are logical sizes, not heap-memory measurements.

### Evaluator: 24 cases

`evaluator.rs` uses the public `evaluate_stateless` entry point; the internal
`evaluate` function is not exported. For each input, both `payload` and
`bench.payload` are measured in three operations:

| Operation | Timed work |
| --- | --- |
| `evaluated_drop` | Evaluate a column and destroy the `Evaluated` result |
| `value_drop` | Evaluate a column, convert the result into an owned `Value`, and destroy it |
| `equality_drop` | Evaluate `payload = payload` (or its qualified equivalent), including both operands, compare, and destroy the result |

Rows, columns, and expression plans are prepared outside timing. Each iteration
constructs a `RowContext::Data` that borrows the prepared row. The public entry
point wraps it in an `Rc`, evaluates the expression, and releases the local
context. Context construction, internal `Rc` allocation/destruction, identifier
lookup, value cloning, comparison where applicable, error-result handling, and
result destruction are included. Original row preparation/destruction is
excluded. This is a public-entry-point baseline, not isolated clone timing or a
measurement of the internal query evaluator alone. Do not subtract the #2049
microbenchmarks to attribute the remainder to an individual operation.

In the current code, identifier evaluation produces an owned `Value`. Its
conversion in `value_drop` moves the already-owned value rather than performing
a second deep copy. Comparing these cases is therefore not evidence of a
separate copy per conversion. Equality includes comparison work as well as two
column evaluations.

Untimed assertions check both identifier forms, their equality results, NULL
propagation, and exact errors for a missing column or alias. Inputs and returned
results pass through `std::hint::black_box` inside timing, with explicit result
destruction.

### MemoryStorage queries: 16 cases

The `query_evaluation` target uses a separate `MemoryStorage` instance per input.
Each has a `bench` table with `id` and `payload`, 1,000 rows numbered 0 through
999, and an identical payload in every row. Schema creation and direct
`StoreMut::append_data` calls occur before timing. SQL is planned once per case.

| Operation | Query | Expected output |
| --- | --- | --- |
| `integer_filter` | `SELECT id FROM bench WHERE id >= 500` | IDs 500–999 |
| `projection` | `SELECT payload FROM bench` | 1,000 single-payload rows |
| `repeated_projection` | `SELECT payload, payload FROM bench` | 1,000 rows, two payload copies each |
| `payload_equality` | `SELECT id FROM bench WHERE payload = payload` | IDs 0–999 |

Each timed iteration calls `Glue::execute_stmt` with the prepared plan, passes
the owned result through `black_box`, and destroys it. Parsing, translation,
planning, table preparation, and final storage destruction are excluded.
Storage scanning, context creation, expression evaluation, owned result
construction, and result destruction are included. Assertions before measuring
check label/row widths, all expected IDs (without relying on result order), and
every payload value in both projection cases.

`MemoryStorage::scan_data` clones its stored rows, including every payload even
when only `id` is selected. Consequently the integer-filter cases also include
large-payload scan copies. Query timings cannot isolate evaluator cloning.
Repeated projection includes additional result copying and destruction;
payload equality returns small IDs but includes deep value comparisons.

## Reproduction and validation

Run from the repository root, in order:

```sh
cargo clippy --all-targets -- -D warnings
cargo fmt --all
cargo bench -p gluesql-core --bench evaluator -- --test
cargo bench -p gluesql_memory_storage --bench query_evaluation -- --test
cargo bench -p gluesql-core --bench evaluator
cargo bench -p gluesql_memory_storage --bench query_evaluation
```

The test-mode commands execute each case once with its untimed assertions.
Full runs use Criterion 0.3 defaults: three-second warm-up, five-second target
measurement, and 100 samples per case. Run targets sequentially to avoid
competing measurement workloads. Reports are written relative to each package:
`core/target/criterion` and `storages/memory-storage/target/criterion`.

Record compiler, CPU, OS, commit, working-tree changes, input sizes, and exact
commands with results. Preserve confidence intervals and outlier observations;
use no automatic performance threshold. Repeat failed measurements or cases
with substantial dispersion and retain both original and repeat results.

## Ownership investigation

The following findings are based on source inspection, not a compiled borrowing
prototype. They identify requirements for a future optimization rather than
prove that changing two `Cow` constructors is sufficient.

| Boundary | Owner and current copying | Borrowing requirements and limits |
| --- | --- | --- |
| Plain/qualified identifiers | `RowContext` resolves a `&Value`; `evaluate_inner` returns `Cow::Owned(value.clone())` | Tie the context reference lifetime to the evaluated result; all recursive evaluation and function paths must support that shorter lifetime |
| `RowContext::Data` | `Cow<Row>` may own or borrow its row; columns use `Rc` | A value reference cannot outlive either the owned context or the borrowed row; both cases must be checked |
| `RefVecData` | Borrows column/value slices | Underlying slices may outlive the wrapper, but current getters tie the result to the context borrow; this does not automatically let results escape a local wrapper |
| `Bridge` and `Data::next` | Own child-context `Rc`s and forward lookup | Keep the owning context chain alive throughout use; preserve lookup precedence and alias semantics |
| Filter comparisons | Local/row contexts survive the synchronous `check_expr` call; results become booleans | Strong candidate for consuming borrowed operands before context destruction; equality already reads through `as_ref`, but lifetime propagation still needs validation |
| Projection | Evaluated expressions convert into owned `Value`s for output rows | `Cow::into_owned` copies borrowed values; direct column projection may retain one required output copy even after borrowing intermediate values |
| Public stateless evaluation | `evaluate_stateless` creates and drops a local `Rc<RowContext>` | Results must not borrow local owned context data after return; preserve the public signature or explicitly assess compatibility |
| Custom functions | Arguments form a locally owned row; body/default plans are also local | Materialize any escaping result before local owners disappear; retain expression type semantics and argument casts |
| Scalar subqueries | Query execution produces owned rows; a value is moved from the single result row | Preserve cardinality/error rules and ownership of the extracted value; this is not the same identifier-clone opportunity |
| Aggregates | `Rc<AggregateValues>` owns a boxed value slice; slot lookup is cloned | A borrowing change must also bound the aggregate reference lifetime; direct projected aggregate results still require owned output |

Relevant source paths are
[`evaluate.rs`](../src/executor/evaluate.rs),
[`RowContext`](../src/executor/context/row_context.rs),
[`Evaluated` conversions](../src/executor/evaluate/evaluated.rs),
[`filter`](../src/executor/filter.rs),
[`query filtering`](../src/executor/query/filter.rs),
[`projection`](../src/executor/query/project.rs), and
[`MemoryStorage`](../../storages/memory-storage/src/lib.rs).

In particular, `evaluate` currently accepts a context reference whose borrow
lifetime is not explicitly tied to the returned `Evaluated` lifetime. A future
change would need to propagate this relationship through `evaluate_inner`,
`evaluate_function`, and callers. `RowContext` getters likewise require a
review of their receiver and backing-data lifetimes.

If a stateless or custom-function boundary promotes a borrowed result to owned
data, it must preserve `Evaluated::Number`, `Text`, `StrSlice` (including its
range), and `Value` semantics. Converting every result to a `Value` is not a
neutral replacement: for example, numeric literals use `BigDecimal` evaluation
before later conversion. No ownership-promotion helper is implemented here.

## Next optimization candidate

First investigate borrowing identifier operands consumed by condition
evaluation, preserving owned results at escaping boundaries. Validate normal
and qualified identifiers, `Data`/`RefVecData`/`Bridge` lookups, NULL behavior,
missing-column/alias errors, functions, subqueries, aggregate slots, and output
rows. Use the same evaluator and query targets before and after a candidate
change. A stateless column-returning case may still need copying at its return
boundary, while comparison can potentially avoid deep operand copies.

Storage scan copies and owned output construction may hide or outweigh an
evaluator improvement. These baselines do not quantify row memory/layout or
establish a speedup from `Rc<Value>` storage. Row allocation, final destruction,
memory layout, and realistic varying-payload workloads remain later work.

See [the initial measurements](EVALUATION_RESULTS.md) for all 40 timings,
confidence intervals, environment details, and observed limits.
