# Adding observability

Observations follow function boundaries. Start with one attribute; when separate operations
need separate timings, extract cohesive functions and instrument them individually. See
[Observability](index.md) for setup, profiles, and existing measurement boundaries.

Keep instrumentation in core or the storage implementation, and subscriber initialization
in the CLI, example, or consuming application. Adding an observation point does not require
selecting an output format or adding a subscriber dependency to core.

## Observe a function

```rust
#[cfg_attr(feature = "tracing", gluesql_core::observe)]
fn validate_rows(rows: &[i32]) -> Result<(), &'static str> {
    if rows.iter().any(|row| *row < 0) {
        return Err("negative row");
    }
    Ok(())
}
```

This creates a DEBUG span named `validate_rows` under the `gluesql` target. It measures the
whole function and closes on normal return, early return, error propagation with `?`, or
unwinding. Arguments, successful results, and errors are not recorded automatically.

Core re-exports `observe`. Consuming crates enable `gluesql-core/tracing` and an
optional direct `tracing` dependency through their feature, as shown in the storage setup below.
Use `cfg_attr` so tracing-disabled builds compile the original function. Existing
`gluesql_macros` imports are also supported. The macro does not install a subscriber.

Sync and async non-const functions and methods are supported. Async spans are entered only while
the future is polled. Iterator-producing functions measure preparation and any eager processing;
later consumption belongs to the consuming function's span. See
[Storage access and lazy work](index.md#storage-access-and-lazy-work) for concrete boundaries.

### Split work into observed functions

```rust
#[cfg_attr(feature = "tracing", gluesql_core::observe)]
fn collect(input: &[i32]) -> Result<Vec<i32>, &'static str> {
    let rows = copy_rows(input);
    validate_rows(&rows)?;
    Ok(rows)
}

#[cfg_attr(feature = "tracing", gluesql_core::observe)]
fn copy_rows(input: &[i32]) -> Vec<i32> {
    input.to_vec()
}
```

Together with `validate_rows` above, successful and failing calls produce:

```text
collect
├── copy_rows
└── validate_rows
```

The parent measures the complete operation; children measure copying and validation. Validation
errors propagate unchanged.

Run the complete example, which installs a DEBUG subscriber and prints busy and idle time on
span close:

```sh
cargo run -p gluesql-macros --example observe
```

The example uses `#[observe]` directly with development dependencies and needs no feature flag.
Its subscriber has a fixed filter independent of `RUST_LOG`.

`observe` accepts no options: names come from functions, the target is `gluesql`, and the
level is DEBUG. Raw identifier prefixes are omitted: `fn r#type()` creates `type`.
Observations are read in their calling hierarchy. To measure another operation, introduce a
function boundary rather than adding fields or selecting statements inside the function.

## Observe storage methods

Enable tracing through core; a separate `gluesql-macros` dependency is unnecessary:

```toml
[features]
tracing = ["dep:tracing", "gluesql-core/tracing"]

[dependencies]
tracing = { version = "0.1", optional = true }
```

Apply `observe` to each method containing the actual operation:

```rust
impl Store for MyStorage {
    #[cfg_attr(feature = "tracing", gluesql_core::observe)]
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        // Existing method body.
    }
}
```

The method creates a DEBUG span named `fetch_schema`, using the same instrumentation as any
other observed function. Arguments, results, errors, batch counts, and iterator-consumption
timings are not captured automatically. Values need no Debug bound.

Redb observes its operational `StorageCore` methods; its public trait implementations only
delegate and add no wrapper spans. Observe a shared default implementation directly, as core
does for `Planner::plan`. An override needs its own observation. To measure internal operations
separately, extract cohesive functions and apply `observe` to them.

For a storage exposed as an optional facade dependency, add `"<storage-dependency-name>?/tracing"`
to the `tracing` feature in `pkg/rust/Cargo.toml`. Use the dependency key, including any rename.
Check the storage with and without tracing, and the facade with both storage and tracing enabled.
Applications install a subscriber for both calls through `Glue` and direct storage calls.

## Extend the resource benchmark

Use Redb's [resource benchmark](index.md#resource-benchmark-profiles) as the reference. In
`storages/redb-storage/`, start from `examples/resource_benchmark.rs` and the example
registration in `Cargo.toml`. Firefox profile conversion lives separately in
`cli/src/firefox_profile.rs`, registered by the CLI subscriber.

When a subscriber is used only by tests and examples, add it as a development dependency:

```sh
cargo add tracing-subscriber --dev --features env-filter
```

This is how the macros and Redb packages use it. Storage implementation code only generates
spans; the application selects and registers the subscriber.

Adapt the storage construction, arguments, workload, and persistent-size measurement. Keep
benchmark field names unchanged for comparison.
In-memory and remote storages should leave the local persistent-size field empty when it is
not measurable. Register the example with `required-features = ["tracing"]` and keep profiling
dependencies optional or limited to development builds. Verify formatted resource measurements
and a tracing-disabled build; compare workloads in fresh processes.
