# Adding observability

Use this guide when adding observation points to GlueSQL functions, integrating a storage, or
creating a resource benchmark for another storage. For enabling tracing and interpreting the
existing execution spans, start with [Execution stages and interpretation](index.md).

Declare observations with `cfg_attr(feature = "tracing", ...)` so the original function is
compiled without instrumentation when tracing is disabled. A named observation normally covers
the function call; `start` and `end` select a smaller interval. Field hooks record values at
selected points but do not create separate timing intervals. Choose the interval first, then
record the counts needed to interpret its cost.

## Declaring function observations

### Run a complete example

From the root of this GlueSQL checkout, copy and run:

```sh
cargo run -p gluesql-macros --example observe
```

This runs `macros/examples/observe.rs`; no database, external collector, or extra installation
is needed beyond the repository's Rust build prerequisites. It demonstrates a successful call,
an early error return, and a span covering only part of a function. Both function bodies remain
free of tracing calls; their attributes declare the observation points.

The example installs a console subscriber at DEBUG level, which includes `observe`'s default
DEBUG spans and ERROR events. `FmtSpan::CLOSE` prints the final recorded fields when each span
closes. ANSI colors and time output are disabled to keep the output easy to compare. The level
is fixed in code rather than selected with `EnvFilter`, so `RUST_LOG` does not change this
example's output.

To save its logs in your home directory while leaving the check result on the terminal:

```sh
cargo run --quiet -p gluesql-macros --example observe 2> "$HOME/gluesql-observe-example.log"
less "$HOME/gluesql-observe-example.log"
```

Cargo diagnostics also use stderr and may appear in that file. The program prints
`All observation example checks passed.` to stdout on success. stdout and stderr may be
displayed in a different order when combined. The assertions check the functions' return values;
they do not automatically validate the emitted fields. Observation assertions are covered by
`macros/tests/runtime_observe.rs`.

It exercises these three cases:

| Call | Expected observation |
| --- | --- |
| `collect(&[1, 2, 3])` | `gluesql.example.collect` closes with `input_rows=3`, `buffered_rows=3`, `scanned_rows=3`, and `returned_rows=3`. |
| `collect(&[1, -2, 3])` | An error event contains `negative row`; the span closes with `scanned_rows=2` and no recorded `returned_rows` value. |
| `count_keys(&[1, 2, 3])` | `gluesql.example.collect_keys` closes with `buffered_rows=3`; work after `let num_keys` is outside this span. |

The tracing output is:

```text
DEBUG gluesql.example.collect{input_rows=3 buffered_rows=3 scanned_rows=3 returned_rows=3}: gluesql: close
ERROR gluesql.example.collect{input_rows=3 buffered_rows=3 scanned_rows=2}: gluesql: error="negative row"
DEBUG gluesql.example.collect{input_rows=3 buffered_rows=3 scanned_rows=2}: gluesql: close
DEBUG gluesql.example.collect_keys{buffered_rows=3}: gluesql: close
```

#### Whole-function observations

The first function copies its input, rejects negative values, and returns the copied rows:

```rust
#[gluesql_macros::observe(
    name = "gluesql.example.collect",
    fields(input_rows = input.len()),
    after_let(rows, record(buffered_rows = rows.len())),
    count_loop(binding = row, field = scanned_rows),
    on_ok(rows, record(returned_rows = rows.len())),
    err(Debug)
)]
fn collect(input: &[i32]) -> Result<Vec<i32>, &'static str> {
    let rows = input.to_vec();
    for row in &rows {
        if *row < 0 {
            return Err("negative row");
        }
    }
    Ok(rows)
}
```

Each attribute option observes a different point in that execution:

| Option | Observation point | Recorded information |
| --- | --- | --- |
| `name` | The span surrounding the function call | The operation name `gluesql.example.collect` |
| `fields(input_rows = input.len())` | Span creation | Number of input items |
| `after_let(rows, record(buffered_rows = rows.len()))` | Immediately after `let rows = input.to_vec()` succeeds | Number of copied items retained in the buffer |
| `count_loop(binding = row, field = scanned_rows)` | Each entry into the selected `for row` loop body | Number of items whose inspection started |
| `on_ok(rows, record(returned_rows = rows.len()))` | After the function returns `Ok` | Number of successfully returned items |
| `err(Debug)` | After the function returns `Err` | An ERROR event containing the error's `Debug` representation |

The field names are chosen by the example, not reserved metric names interpreted by the macro.
In `on_ok`, `rows` names a reference to the successful return value; it does not select the local
variable named `rows`. Renaming that local variable therefore requires updating `after_let`, but
does not require changing `on_ok`.

For `[1, 2, 3]`, all four counts are 3: the function receives three items, copies three items,
enters the loop three times, and returns three items. The counts happen to agree because this
function does not filter its input.

For `[1, -2, 3]`, copying still finishes before validation, so `input_rows` and `buffered_rows`
remain 3. The loop enters twice and returns an error while inspecting `-2`; the third item is
never inspected. `scanned_rows` is therefore 2, including the item that failed validation.
To count only iterations that reach a particular successful initialization, use
`increment = after_let(...)`, as in the `validate_unique` example below.

The error call has no recorded `returned_rows` value because `on_ok` does not run for `Err`.
This differs from a successful empty result, which records `returned_rows=0`. The partial loop
count is preserved on early return, and the function span still closes. `err(Debug)` emits an
event; it does not turn the error into a successful result or suppress its propagation.

All these counts describe items, not allocated bytes or RSS.

#### Observing part of a function

The second function measures copying and counting without including the subsequent assertion
or return:

```rust
#[gluesql_macros::observe(
    name = "gluesql.example.collect_keys",
    start = before_let(keys),
    end = after_let(num_keys),
    record(buffered_rows = num_keys)
)]
fn count_keys(input: &[i32]) -> usize {
    let keys = input.to_vec();
    let num_keys = keys.len();
    // This work is outside the collection span.
    assert_eq!(num_keys, input.len());
    num_keys
}
```

`start = before_let(keys)` opens the span immediately before the buffer is initialized.
`end = after_let(num_keys)` records `buffered_rows` and closes the span immediately after the
length is assigned. Both the copy and the length calculation are inside the observed interval;
`assert_eq!` and the final return are outside it. The function returns `usize`, so this example
uses a range-end `record` rather than `on_ok`.

Range observations let an existing function expose the cost of one preparation or collection
phase without extracting a helper function. An early return before the endpoint still closes
the span, but does not perform the endpoint's final record.

This macro-crate example deliberately uses the attribute directly and the crate's existing
development dependencies, so it needs no `--features tracing` flag. In a consuming crate,
keep tracing optional using `cfg_attr` and optional dependencies as described below. The
remaining snippets are integration patterns, not standalone programs.

### Integrate with an existing function

Use `gluesql_macros::observe` to keep instrumentation out of function bodies. The attribute
supports non-const functions and methods. Synchronous functions support all selectors below;
async functions support whole-function spans with `name`, `level`, `target`, `fields`, and
`err(Debug)`, entering the span only while their future is polled. Enable optional `gluesql-macros` and
`tracing` dependencies through the consuming crate's `tracing` feature, as in the storage setup
below. Always use `cfg_attr`: without the feature, the original function is compiled without
generated spans, counters, or field expressions.

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(name = "gluesql.evaluate", level = "trace")
)]
fn evaluate(/* existing arguments */) -> Result<Evaluated<'_>> {
    // Existing implementation.
}
```

`name` is required for spans; event-only observations omit it. `level` defaults to `debug` and accepts `trace`, `debug`, `info`, `warn`, or
`error`; `target` defaults to `gluesql`. A function observation ends on normal return, early
return, error propagation with `?`, or unwinding. It measures the function call, not subsequent
consumption of a returned iterator. Continue using `trace_storage` for lazy storage iterators.
The attribute does not install a subscriber.

### Initial fields and local values

Use `fields` for values available when the observation starts:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        name = "gluesql.storage.lookup",
        fields(backend = "example", table = table_name, key = ?key)
    )
)]
fn lookup(/* existing arguments */) -> Result<Option<DataRow>> {
    // Existing implementation.
}
```

`?value` records `Debug` formatting and `%value` records `Display` formatting. Expressions are
borrowed, not consumed, and are evaluated only when the generated span is enabled.

Use `after_let` for values available inside the function. The macro declares the recorded fields
automatically and inserts the recording immediately after the selected initialization succeeds:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        name = "gluesql.query.aggregate",
        after_let(rows, occurrence = 2, record(buffered_groups = rows.len()))
    )
)]
fn execute(/* existing arguments */) -> Result<AggregatedRows<'_>> {
    // Existing implementation, including the original `let rows` declarations.
}
```

Selectors match bound identifiers, including tuple and struct destructuring. They search the
function's blocks in source order, counting a declaration before its initializer's nested blocks.
Parameters, separate item definitions, closure bodies, async blocks, and macro token bodies are
not searched. A `let` must have an initializer; `if let` and `while let` conditions are not targets.
Generated hooks inherit the selected statement's `cfg` and `cfg_attr` attributes. A declaration
excluded from compilation does not leave a dangling field expression behind. Occurrence numbers
still count declarations in source order, including conditionally excluded declarations.
Raw field identifiers such as `r#type` are supported and recorded as `type`.

- Omit `occurrence` when exactly one declaration matches.
- Use `occurrence = N` to select a declaration, counting from 1.
- Use `all` to record after every matching declaration, including declarations in separate branches.
- Repeat `after_let(...)` to select multiple specific declarations or record different fields.

Repeated writes to one span field retain its latest value; they are not time-series samples.
If initialization returns through `?`, the subsequent record is not reached. If an initializer
moves a value into an iterator, select an earlier point where the desired buffer still exists.

Use `after_loop(row, record(buffered_rows = rows.len()))` to record after a `for row in ...` loop,
as in the hash-join build. This selector also accepts `occurrence` or `all`. It records after
normal completion or a local `break`, but not when the function returns from inside the loop.

### Measuring a partial function interval

Pair `start` and `end` to measure a region without extracting a new function. DELETE uses:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        name = "gluesql.mutation.collect",
        fields(operation = "delete"),
        start = before_let(keys),
        end = after_let(num_keys),
        record(buffered_rows = num_keys)
    )
)]
fn delete(/* existing arguments */) -> Result<Payload> {
    // Existing setup.
    let mut keys = Vec::new();
    // Existing key collection and foreign-key validation.
    let num_keys = keys.len();
    // Existing storage mutation.
}
```

Both endpoints accept `before_let` or `after_let`, with an optional `occurrence`. They must select
ordered positions in the same block. The span starts only if execution reaches the start point.
The top-level `record` runs at the end point, before the span is exited and closed. On earlier
return or unwinding, the span closes without that final record. The storage mutation stays
outside the collection span.
Endpoints with their own `cfg` or `cfg_attr` are rejected, since separately conditional endpoints
can leave a span without a matching start or end. Put the condition on their enclosing block or
function instead.

Range observations support `fields` and the final `record`; they cannot be combined with
`after_let`, `after_loop`, `count_loop`, `on_ok`, or `err(Debug)` in the same attribute. Invalid
combinations produce a compile error rather than silently omitting error events.

### Counting loop iterations

`count_loop(binding = row, field = scanned_rows)` counts entries into a selected `for row in ...`
body, including iterations that immediately propagate an error. Add an increment point to count
only after a particular initialization succeeds:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        name = "gluesql.validate.unique",
        count_loop(
            binding = row,
            increment = after_let(values),
            field = scanned_rows
        )
    )
)]
fn validate_unique(/* existing arguments */) -> Result<()> {
    for row in storage.scan_data(table_name)? {
        let (_, values) = row?;
        // Existing validation.
    }
    Ok(())
}
```

Loop selection accepts `occurrence = N` when its binding is ambiguous. The optional increment
selector is resolved within that loop body and must identify one declaration. The counter is
recorded when leaving the loop's surrounding execution region, including error return and
unwinding. Unlike the previous success-only validation record, errors retain the partial scan
count. A loop that executes zero iterations records zero; an unreached loop records nothing.
Instrumentation never pulls additional items from the iterator.

### Recording successful return values

`on_ok` binds a reference to the successful `Result` value and records without cloning or
consuming it:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        name = "gluesql.insert.collect",
        on_ok(rows, record(buffered_rows = rows.len()))
    )
)]
fn fetch_rows(/* existing arguments */) -> Result<Vec<Vec<Value>>> {
    // Existing implementation.
}
```

Explicit successful `return` statements are included. Errors pass through unchanged without the
success record. The return type must be written as `Result<...>` (optionally qualified); aliases
with other names are not supported. Mutable references and opaque success types such as
`Result<impl Iterator<Item = Row>, Error>` retain their original return semantics.
Add `err(Debug)` to emit an error event for a returned `Err`. `trace_storage` uses this same
function observation generator for method spans, argument fields, and error events; its iterator
wrapper continues to handle lazy consumption separately.

### Declaring events without a span

Use `event` without `name` when only an event is needed. This preserves the current parent span
and does not add a new span to the hierarchy:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        event("selected query access path", access_path = "full_scan")
    )
)]
fn fetch(/* existing arguments */) -> Result<KeyedRows<'_>> {
    // Existing implementation.
}
```

Events can also be attached to precise statement boundaries:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        after_let(key, event("selected query access path", access_path = "primary_key"))
    )
)]
fn rows(/* existing arguments */) -> Result<SourceRows<'_>> {
    // Existing implementation.
}
```

`before_let` emits before initialization; `after_let` emits after successful initialization;
`after_loop` emits after loop completion. All accept the same occurrence selection rules as
recording hooks. Entry events and hooks use the attribute's `level` and `target`. Event expressions
are evaluated only when the event is enabled. Access-path events use these attributes and leave
the SQL execution bodies free of logging calls.

### Validation when changing observed code

Missing or ambiguous targets, invalid occurrence numbers, unsupported options, and invalid
range boundaries produce macro errors. Rust checks expression types and visibility at each
injected location. A refactor can change which declaration an occurrence selects even when it
still compiles, so review the attribute alongside the body and verify the recorded values.

Run checks with `tracing` enabled as well as disabled. Selectors are not checked when `cfg_attr`
disables the macro. Runtime tests in `macros/tests/runtime_observe.rs` cover control flow, disabled
field evaluation, and query-pipeline buffer counts.

## Adding tracing support to a storage

A storage opts in without changing its method bodies or any GlueSQL call sites. The procedural
macro instruments the methods explicitly defined in each attributed `impl` block.

Enable core tracing and add the optional `tracing` dependency to the storage crate. Core
re-exports `trace_storage` and `observe` under this feature, so a separate `gluesql-macros`
dependency is not required:

```toml
[features]
tracing = ["dep:tracing", "gluesql-core/tracing"]

[dependencies]
tracing = { version = "0.1", optional = true }
```

Apply the attribute to each implemented trait without changing the method bodies or call sites:

```rust
#[cfg_attr(feature = "tracing", gluesql_core::trace_storage(name = "my_storage", capture = "off"))]
impl Store for MyStorage {
    // Existing implementation
}
```

The default `capture = "full"` records every simple named argument with its `Debug`
representation and records `Result` errors. Start with `capture = "off"` to keep timing and
counts without argument, row, or error values. `StoreMut::append_data` and `insert_data` record
`rows.len()`, and `StoreMut::delete_data` records `keys.len()`, as `row_count` in either mode.
Other methods do not infer `.len()` from argument names. The generated span name follows
`gluesql.<storage>.<method>`. The default remains `capture = "full"` for compatibility. With
`capture = "off"`, iterator rows and errors do not need to implement `Debug`.

Use `skip(new, helper)` in the outer attribute to leave selected methods unchanged, for example
generic constructors whose arguments do not implement `Debug`. Skipped names must exist in the
implementation and cannot also be selected with `iterators(...)` or `#[trace_iterator]`.

`Store::scan_data`, `Index::scan_indexed_data`, and `Metadata::scan_table_meta` results are wrapped
automatically when the implemented trait path ends in `Store`, `Index`, or `Metadata`, respectively.
For renamed trait imports, select the method explicitly with `iterators(...)`. Inherent methods with the same
names are not automatically wrapped. The wrapper emits each
yielded row or error as an event when capture is enabled and records `row_count`, `error_count`,
and `completed` when dropped. Select other iterator-returning methods in the outer attribute:

```rust
#[cfg_attr(feature = "tracing", gluesql_core::trace_storage(name = "my_storage", iterators(stream)))]
impl MyStorage {
    fn stream(&self) -> Result<Box<dyn Iterator<Item = Result<Row>>>> {
        // Existing implementation
    }
}
```

This leaves no helper attributes behind when the feature is disabled. Method names must exist
in the attributed implementation and cannot be repeated. The legacy `#[trace_iterator]` marker
is still accepted inside an unconditionally instrumented implementation; prefer `iterators(...)`
with feature-gated instrumentation.

The generated code still requires a direct dependency named `tracing`. Existing imports from
`gluesql_macros` remain supported. If the storage is exposed as an optional dependency of the
`gluesql` package, also append `"<storage-dependency-name>?/tracing"` to its existing `tracing`
feature in `pkg/rust/Cargo.toml`; use the dependency key, including any underscores or rename.
The `?` forwards tracing only when that storage is enabled and does not enable the storage itself.
Likewise, storage wrappers can forward `"<inner-storage>/tracing"` once their inner storage
provides that feature.

Treat the Cargo feature, attributes on explicitly implemented trait methods, and facade feature
forwarding as one integration change. Before shipping a new integration, check the storage with
and without its `tracing` feature and check the facade with that storage and `tracing` enabled.

The attribute works on inherent implementations and external traits as well as GlueSQL storage
traits. Iterator methods must return a `Result` whose success value is a boxed iterator of
`Result` items. No subscriber setup is needed when calls begin through `Glue`; direct calls made
before `Glue::new` use any subscriber already installed by the application.

The automatically wrapped storage scan methods return lazy iterators. Their method spans measure
iterator creation, while the generated iterator spans measure consumption. Their iterator span suffixes are `scan_rows`,
`scan_indexed_rows`, and `scan_table_meta_rows`, respectively.

## Adding a benchmark for another storage

The [Redb resource benchmark](tools.md#resource-benchmark-profiles) is currently the
reference implementation rather than a shared benchmark harness.
To add the same measurements to another storage crate, use these files as the starting point:

```text
storages/redb-storage/examples/resource_benchmark.rs
storages/redb-storage/examples/resource_benchmark/firefox_profile.rs
storages/redb-storage/examples/resource_benchmark.sql
storages/redb-storage/Cargo.toml
```

First add a `tracing` feature to the target storage crate. It must enable the optional `tracing`
dependency together with `gluesql-core/tracing`; use the macros re-exported by `gluesql_core`.
Register the example with
`required-features = ["tracing"]` so its tracing and RSS dependencies are not compiled for normal
storage users:

```toml
[features]
tracing = ["dep:tracing", "gluesql-core/tracing"]

[dependencies]
tracing = { version = "0.1", optional = true }

[dev-dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }

[target.'cfg(unix)'.dev-dependencies]
libc = "0.2"

[[example]]
name = "resource_benchmark"
required-features = ["tracing"]
```

Copy `resource_benchmark.rs` and adapt only the storage-specific parts:

1. Import and construct the target storage instead of `RedbStorage`.
2. Change `benchmark.storage` from `"redb"` to a stable storage name.
3. Adjust the command-line arguments required to create or connect to the storage.
4. Record the storage size only when it can be measured locally and consistently.
5. Add a representative SQL file and pass the complete document to `Glue::execute(&sql)`.

Keep the following names and behavior unchanged so profiles remain comparable across storage
implementations:

```text
gluesql.benchmark.run
gluesql.benchmark.memory_sample
benchmark.name
benchmark.storage
process.memory.peak_bytes
gluesql.database.size_bytes
process.executable.size_bytes
GLUESQL_MEMORY_SAMPLE_MS
```

Use the storage type to decide how `gluesql.database.size_bytes` is populated:

| Storage type | Size measurement |
| --- | --- |
| Single local file | Read the database file metadata after the workload finishes |
| Local directory | Sum only the files owned by that database after pending writes are flushed |
| In-memory | Leave `gluesql.database.size_bytes` empty |
| Remote service | Leave the local field empty; report a server-side metric separately if available |

Apply `trace_storage` as described above when storage calls and their arguments must be visible.
The macro uses one span for lazy iterator consumption, records its final row count, and emits row
events rather than creating a span for every row.

Firefox Profiler support is optional. To include it, copy the `firefox_profile.rs` support module
without changing its marker and counter names, retain `GLUESQL_FIREFOX_PROFILE_PATH` as the output
setting, and add these optional dependencies:

```toml
[features]
firefox-profile = [
  "tracing",
  "dep:fxprof-processed-profile",
  "dep:serde_json",
]

[dependencies]
fxprof-processed-profile = { version = "0.8.1", optional = true }
serde_json = { version = "1", optional = true }
```

Validate the new example in both modes:

```sh
cargo run --release \
  -p <storage-package> \
  --example resource_benchmark \
  --features tracing \
  -- <storage-arguments> <workload.sql>

GLUESQL_FIREFOX_PROFILE_PATH=~/gluesql-benchmark-profile.json \
RUST_LOG=gluesql=debug \
cargo run --release \
  -p <storage-package> \
  --example resource_benchmark \
  --features firefox-profile \
  -- <storage-arguments> <workload.sql>
```

Confirm that the formatted trace contains `gluesql.benchmark.run`, the Firefox profile contains
GlueSQL markers and a `process_rss` counter, and a tracing-disabled build of the storage remains
unchanged. Run each comparison in a fresh process with an equivalent workload and build profile.

