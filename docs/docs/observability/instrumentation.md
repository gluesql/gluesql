# Adding observability

Use this guide to instrument GlueSQL functions, integrate a storage, or create a resource
benchmark for another storage. See [Execution stages and interpretation](index.md) for the
existing measurement boundaries and [Tools and setup](tools.md) for subscribers and exporters.

Observations follow function boundaries. Start with one attribute on a function. If a function
contains separate operations whose costs need to be distinguished, extract those operations
into cohesive functions and instrument them individually. The macro does not locate statements,
rewrite loops, inspect return values, or choose intervals inside a function.

## Declaring function observations

### Start with one attribute

```rust
#[cfg_attr(feature = "tracing", gluesql_core::observe)]
fn validate_rows(rows: &[i32]) -> Result<(), &'static str> {
    if rows.iter().any(|row| *row < 0) {
        return Err("negative row");
    }
    Ok(())
}
```

This creates a DEBUG span named `validate_rows` under the `gluesql` target. It covers the whole
function and closes on normal return, early return, error propagation with `?`, or unwinding.
The macro does not automatically record arguments, successful return values, or errors. A span
can therefore close on either success or failure without indicating which result was returned.

Always use `cfg_attr` in consuming crates. Without the feature, the original function is compiled
without instrumentation. Enable `gluesql-core/tracing` and an optional direct `tracing` dependency
through the consuming crate's feature; core re-exports both observation macros. Existing
`gluesql_macros` imports are also supported.

Synchronous and async non-const functions and methods are supported. An async span is entered
only while the future is polled, so suspension does not leave it entered on the calling thread.
For iterator-producing functions, the span measures iterator creation and preparation, including
any eager input processing performed by that function. Iterator consumption time is included in
the consuming function's span, when instrumented. A const function cannot create a runtime span.

### Split work at function boundaries

Run the complete example from the repository root:

```sh
cargo run -p gluesql-macros --example observe
```

The example uses `#[observe]` directly with the macro crate's development dependencies, so it
needs no feature flag. It separates copying and validation into functions:

```rust
#[gluesql_macros::observe]
fn collect(input: &[i32]) -> Result<Vec<i32>, &'static str> {
    let rows = copy_rows(input);
    validate_rows(&rows)?;
    Ok(rows)
}

#[gluesql_macros::observe]
fn copy_rows(input: &[i32]) -> Vec<i32> {
    input.to_vec()
}

#[gluesql_macros::observe]
fn validate_rows(rows: &[i32]) -> Result<(), &'static str> {
    if rows.iter().any(|row| *row < 0) {
        return Err("negative row");
    }
    Ok(())
}
```

Both the successful and failing calls produce this hierarchy:

```text
collect
├── copy_rows
└── validate_rows
```

The parent measures the complete operation, while each child measures one function. On validation
failure, the error propagates through `collect` unchanged and both spans close. No loop counter,
return-value field, or error event is generated.

The example installs a DEBUG subscriber with `FmtSpan::CLOSE`, which prints busy and idle time
when each span closes. Its fixed filter is independent of `RUST_LOG`. Assertions verify the
return values; `macros/tests/runtime_observe.rs` verifies the observations and control flow.
Save the example output with:

```sh
cargo run --quiet -p gluesql-macros --example observe 2> "$HOME/gluesql-observe-example.log"
less "$HOME/gluesql-observe-example.log"
```

### Optional span metadata

Only these options are supported:

| Option | Default | Purpose |
| --- | --- | --- |
| `name = "..."` | Function or method name | Stable operation name or distinction between functions with the same name |
| `target = "..."` | `gluesql` | Subscriber filtering |
| `level = "..."` | `debug` | `trace`, `debug`, `info`, `warn`, or `error` |
| `fields(...)` | No fields | Values available at function entry |

Raw function identifier prefixes are omitted: `fn r#type()` creates a span named `type`.
Execution-layer functions use this default naming and are interpreted in their calling
hierarchy. Explicit names remain available when a custom operation name is needed.

Entry fields can identify the input without selecting an internal observation point:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_core::observe(fields(table = table_name))
)]
fn load_table(table_name: &str) {
    // Existing implementation.
}
```

Use `?value` for Debug formatting and `%value` for Display formatting. Fields use the standard
`tracing::instrument` syntax. Duplicate options, unknown options, and unsupported levels produce
compile errors. The macro does not install a subscriber.

The former `before_let`, `after_let`, `after_loop`, `count_loop`, `start`, `end`, `record`, `on_ok`,
`err(Debug)`, and `event` options are no longer supported. Remove statement selectors and
counters, or extract a function when a separate timing boundary is needed. Access paths are now
entry fields on a function span rather than events injected into selected branches.

## Adding tracing support to a storage

A storage opts in without changing its method bodies or GlueSQL call sites. Enable core tracing
and add the optional direct `tracing` dependency:

```toml
[features]
tracing = ["dep:tracing", "gluesql-core/tracing"]

[dependencies]
tracing = { version = "0.1", optional = true }
```

Apply one attribute to each implemented trait:

```rust
#[cfg_attr(feature = "tracing", gluesql_core::trace_storage)]
impl Store for MyStorage {
    // Existing implementation.
}
```

Each explicitly implemented method gets a TRACE span named `gluesql.<type>.<method>`. The type
name comes from the implementation target: `impl Store for MyStorage` produces
`gluesql.MyStorage.<method>`, and an inherent `impl MyStorage` uses the same name. Module paths
and generic arguments are omitted, and raw identifier prefixes are stripped. An explicit
`name = "my_storage"` overrides this default and produces `gluesql.my_storage.<method>`.
Non-path implementation types, such as tuples or references, require an explicit name. The
macro reuses the same whole-function instrumentation as `observe`. It also works on inherent
implementations and external traits. Arguments and return values need no Debug bound because
no values, errors, batch counts, or iterator counters are captured automatically.

Use `skip(new, helper)` to leave selected methods unchanged, for example const constructors.
Skipped method names must exist in the attributed implementation and cannot be repeated.
Only `name` and `skip` are supported. The former `capture`, `iterators`, and `trace_iterator`
options are removed.

A scan method's span measures iterator creation and preparation. For Redb's normal read path,
this includes read-transaction and table setup and range iterator preparation. Row-reading and
decoding time during iterator consumption is included in the consuming function's span, when
instrumented. No iterator wrapper or additional iterator-consumption span is generated.

In Redb's explicit-transaction path, the scan span also measures the complete row-reading,
decoding, and collection work needed to prepare an iterator over the collected vector.
Subsequent iterator consumption traverses those collected rows. Inspect the implementation to
distinguish iterator preparation time from the work performed during consumption.

The generated code requires a direct dependency named `tracing`. If the storage is exposed as
an optional dependency of the `gluesql` package, append
`"<storage-dependency-name>?/tracing"` to its existing `tracing` feature in `pkg/rust/Cargo.toml`.
Use the dependency key, including any underscores or rename. The `?` forwards tracing only when
that storage is enabled. Wrappers can likewise forward an inner storage's tracing feature.

Check a new integration with and without tracing and check the facade with the storage and
tracing enabled. No subscriber setup is needed when calls begin through `Glue`; direct calls
before `Glue::new` use any subscriber already installed by the application.

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

Apply `trace_storage` as described above when storage method durations must be visible.
The macro measures each method call; it does not wrap returned iterators or record per-row events.

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
