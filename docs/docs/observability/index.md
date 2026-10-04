# Observability

GlueSQL tracing measures query execution at function boundaries. This guide covers enabling
tracing, viewing profiles, and interpreting measurements. See
[Adding observability](instrumentation.md) to instrument functions or storages.

Core and integrated storage implementations define observation points and emit tracing spans.
They do not initialize subscribers, choose output formats, or write profile files. The CLI
or consuming application registers a subscriber to collect those spans and select filters,
formats, destinations, and exporter layers. Subscriber dependencies and output configuration
belong to that application; core depends only on the instrumentation needed to emit spans.

## Enable tracing

Tracing is optional and disabled by default. Build the CLI with the outputs you need:

| Feature | Output |
| --- | --- |
| `tracing` | Formatted span-close events on standard error |
| `tracing-flame` | Formatted events and folded stacks for flamegraphs |
| `opentelemetry` | Formatted events and OTLP traces over HTTP/Protobuf |
| `firefox-profile` | Formatted events and Firefox Profiler JSON |

All exporter features enable `tracing` and can be used together. For example:

```sh
cargo build -p gluesql-cli --features tracing-flame,opentelemetry
```

Run the CLI with environment variables for the outputs you use:

```sh
RUST_LOG=gluesql=debug \
GLUESQL_FLAMEGRAPH_PATH=query.folded \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
./target/debug/gluesql-cli
```

| Environment variable | Purpose | Default |
| --- | --- | --- |
| `RUST_LOG` | Span/event filter | `gluesql=debug` |
| `GLUESQL_FLAMEGRAPH_PATH` | Folded output, with `tracing-flame` | `tracing.folded` |
| `GLUESQL_FIREFOX_PROFILE_PATH` | Firefox JSON output, with `firefox-profile` | `gluesql-profile.json` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector base URL, with `opentelemetry` | `http://localhost:4318` |

The OpenTelemetry SDK also supports `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, which takes precedence
and specifies the complete trace URL. General endpoint URLs receive the `/v1/traces` suffix.
Use standard OpenTelemetry variables for service names and headers. Pending traces and profile files are saved
on CLI exit. Relative output paths use the current working directory; parent directories must
already exist. Unused exporter variables do not enable features.

Use the filter to select measurement detail:

| Level | Measurements under the `gluesql` target |
| --- | --- |
| `info` | Application events and benchmark summaries; core function spans are disabled |
| `debug` | All observed functions and integrated storage methods, including parsing, planning, and execution |

Use `RUST_LOG=gluesql=debug` to include function and storage method spans.
The default in-memory session emits core spans; storage method spans require an integrated
backend such as RedbStorage. Run SQL at the CLI prompt to inspect the function hierarchy.
Append `2> query.log` to the CLI command to save tracing output separately from query results.

### Using GlueSQL as a library

Create a separate Rust application that depends on GlueSQL. Register the subscriber in that
application's initialization code; no changes to GlueSQL's source files are required. Run
the following commands in your application's directory:

```sh
cargo add gluesql --features tracing
```

Applications register a subscriber to choose their output format and destination. For
formatted text with function timings:

```sh
cargo add tracing-subscriber --features env-filter
```

The application uses the subscriber at runtime, so add it as a normal dependency.
GlueSQL core only generates spans through `tracing` and does not depend on `tracing-subscriber`.

```rust
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

tracing_subscriber::fmt()
    .with_env_filter(
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("gluesql=debug")),
    )
    .with_span_events(FmtSpan::CLOSE)
    .with_writer(std::io::stderr)
    .try_init()?;

// Construct Glue and execute queries after registering the subscriber.
```

For JSON logs, add the `json` feature and call `.json()` before `.try_init()`:

```sh
cargo add tracing-subscriber --features env-filter,json
```

For flamegraphs or OpenTelemetry, register the corresponding layers on a subscriber registry.
Applications own subscriber initialization; reuse an existing application subscriber rather
than initializing another global one. Initialize once before the work to be measured.

GlueSQL generates spans without installing a subscriber. Without one, queries still run but
no observations are collected. The CLI and resource benchmark install their own subscribers.

## View profiles

### Flamegraphs from an application subscriber

Add a layer that writes tracing span stacks to a folded file:

```sh
cargo add tracing-subscriber --features env-filter
cargo add tracing-flame
```

Register the layer before executing queries and keep its flush guard alive until the workload
has completed. This replaces the formatted-text subscriber setup above:

```rust
use {
    gluesql::{core::prelude::Glue, gluesql_memory_storage::MemoryStorage},
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (flame, guard) = tracing_flame::FlameLayer::with_file("query.folded")?;
    tracing_subscriber::registry()
        .with(EnvFilter::new("gluesql=debug"))
        .with(flame.with_empty_samples(false))
        .try_init()?;

    let mut glue = Glue::new(MemoryStorage::default());
    glue.execute("SELECT 1")?;
    guard.flush()?;
    Ok(())
}
```

Use an integrated backend such as Redb with its tracing feature enabled to include storage
methods. To also print text, add a `tracing_subscriber::fmt::layer()` to the same registry;
register one subscriber containing all desired layers.

After running the application, convert the folded file to SVG:

```sh
cargo install inferno
inferno-flamegraph --countname nanoseconds < query.folded > query.svg
```

Open `query.svg` in a browser. The layer excludes intervals with no active span, so waiting
outside observed functions does not dominate the graph. `tracing-flame` measures elapsed time
between span events, not sampled CPU usage. The CLI's `tracing-flame` feature registers this
layer for you; exit the CLI to flush its folded output before converting it.

The CLI's OpenTelemetry exporter sends completed spans to an HTTP/Protobuf collector, which
can forward them to Jaeger, Grafana Tempo, or another compatible backend. Library applications
configure their own exporter and subscriber; see the
[OpenTelemetry exporter](https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/),
[OpenTelemetry layer](https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/), and
[flame layer](https://docs.rs/tracing-flame/latest/tracing_flame/) documentation.

### Firefox Profiler through a subscriber layer

Firefox Profiler requires its own profile format; formatted JSON logs from `.json()` cannot
be loaded as a profile. The CLI's `firefox-profile` feature registers a `FirefoxProfileLayer`
alongside its other subscriber layers and saves the JSON when the CLI exits.

Run the benchmark SQL through the CLI with a fresh Redb database:

```sh
GLUESQL_FIREFOX_PROFILE_PATH=/tmp/gluesql-profile.json \
RUST_LOG=gluesql=debug \
cargo run --release -p gluesql-cli --features firefox-profile \
  -- --storage redb --path /tmp/gluesql-profile.redb \
  --execute storages/redb-storage/examples/resource_benchmark.sql < /dev/null
```

Open [Firefox Profiler](https://profiler.firefox.com/), select **Load a profile from file**, and
choose the JSON file. GlueSQL spans appear as interval markers and application events as
instant markers. The profile stays local unless you upload or share it. CLI profiles contain
tracing markers; RSS sampling belongs to the separate resource benchmark below.

The conversion layer is implemented in `cli/src/firefox_profile.rs`. Core and storage crates
have no Firefox profile dependencies. For a separate profiling application, copy this module
into your own project, add its dependencies, and register the layer in your own subscriber:

```sh
cargo add tracing fxprof-processed-profile serde_json
cargo add tracing-subscriber --features env-filter
```

```rust
// Using the copied firefox_profile module in your application.
let profile = firefox_profile::FirefoxProfileLayer::new("query.json".into());
tracing_subscriber::registry()
    .with(tracing_subscriber::EnvFilter::new("gluesql=debug"))
    .with(profile.clone())
    .try_init()?;
// Run the workload and close its spans before saving.
profile.finish()?;
```

Import `tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt}` for `.with()` and
`.try_init()`. The layer is CLI implementation code, not a public GlueSQL API; adapting it in
an application does not require modifying GlueSQL source files.

### Resource benchmark profiles

The Redb resource benchmark separately records RSS samples and final resource measurements
as formatted tracing output. It installs its own subscriber and needs no Firefox exporter:

```sh
RUST_LOG=gluesql=debug \
cargo run --release -p gluesql-redb-storage \
  --example resource_benchmark --features tracing \
  -- /tmp/gluesql-benchmark.redb \
  storages/redb-storage/examples/resource_benchmark.sql
```

| Measurement or setting | Meaning or default |
| --- | --- |
| `gluesql.benchmark.run` | Complete workload; `benchmark.name` is the SQL filename without its extension |
| `process.memory.peak_bytes` | Process-lifetime peak resident set size |
| `gluesql.database.size_bytes` | Persistent storage size after the workload, when measurable |
| `process.executable.size_bytes` | Benchmark executable size |
| `gluesql.benchmark.memory_sample` | Current RSS with `elapsed_ms` and `rss_bytes` |
| `GLUESQL_MEMORY_SAMPLE_MS` | Positive sampling interval in milliseconds; default `10` |

At `info`, only final resource fields are recorded; `debug` and `trace` also enable RSS sampling.
Current RSS sampling supports macOS and Linux; peak RSS supports Unix. Samples include sampler
thread overhead. Compare fresh processes and database paths using the same build profile and
platform, because peak RSS is a process-lifetime high-water mark.

## Follow a query through its spans

`Glue::execute` delegates to `execute_with_params`, whose span covers the complete SQL string.
It includes parsing, parameter conversion, and translation, planning, and execution of each
statement. Core observations use default function names and record no argument fields.

Execution-layer spans use their function names, such as `execute`, `rows`, and `sort`.
Read them in their calling hierarchy; the function paths in the tables below identify the
corresponding implementation. Multiple operators can have the same function name, so the
name alone does not identify the operator.

An illustrative SELECT hierarchy with Redb tracing enabled is:

```text
execute_with_params
├── parse
├── translate_with_params
├── plan
└── execute
    ├── begin
    ├── execute
    │   ├── rows
    │   │   └── scan_data
    │   ├── sort
    │   └── ...
    └── commit
```

The children depend on the query plan and storage integration. For a simple table query with
ORDER BY on Redb's normal read path, `scan_data` measures storage iterator
creation and preparation. `sort` measures input iterator consumption together
with sorting. Storage reads and decoding during consumption are included in the order-by span;
they do not create or extend a storage span.

Parsing happens once per SQL string. Translation, planning, and execution repeat per statement,
so later statements see changes made by earlier statements. A separate call to `Glue::plan` or
`plan_with_params` measures the complete planning call, including parsing and translation.
The default `Planner::plan` implementation is observed directly as `plan`, including when
Redb inherits it or it is called outside `Glue`. Within an execute trace, it measures the
per-statement planning work. `Glue::execute_stmt` delegates to the observed executor `execute`.

Parent and child timings overlap. Do not add their durations to calculate query time. Formatted
span-close logs report busy time while the span is entered and idle time while it is not. For
async functions, the span is entered while the future is polled; neither duration is a CPU
utilization measurement. A span closes on success or failure without automatically recording
the returned result.

## SELECT and query operators

`execute` measures query-output preparation, iterator consumption, conversion
of each row into the public result representation, and final-vector collection in
`core/src/executor/select.rs::execute`. Its duration includes nested query operations and lazy
work performed during iterator consumption. It is not just the cost of appending result rows.

Other stage spans cover these functions, relative to `core/src/executor/`:

| Span | Function | Included work |
| --- | --- | --- |
| `sort` | `query/order_by/select.rs::sort` | Input collection, sort-key evaluation, sorting, and output iterator preparation |
| `execute` | `query/distinct.rs::execute` | Input preparation and collection, then construction of a lazy duplicate filter |
| `build_rows` | `query/join/hash.rs::build_rows` | Build-side reads, key and filter evaluation, retained-row collection, and lookup-map creation |
| `execute` | `query/aggregation.rs::execute` | Input preparation and consumption, grouping, aggregate accumulation, and group export |

Without ordering expressions, the order-by span measures output iterator preparation without
input-row collection or sorting. The distinct span measures input preparation and collection,
plus duplicate-filter iterator preparation. Duplicate-check time is included later in the
consuming function's span, such as result materialization. Interpret each duration according to
whether it measures iterator preparation or includes iterator consumption.

These spans record execution time, not buffer sizes or processed-row counts. To isolate another
operation, separate it into a function with its own observation instead of selecting a local
variable or loop from an attribute.

## Mutations: preparing data before writing

UPDATE and DELETE use the `collect_update_rows` and `collect_keys` function spans:

| Operation | Observed function | Included work | Subsequent work |
| --- | --- | --- | --- |
| UPDATE | `execute.rs::collect_update_rows` | Fetching selected rows, applying assignments, and collecting modified rows | Uniqueness validation, write-batch conversion, and `insert_data` |
| DELETE | `delete.rs::collect_keys` | Column and referencing-metadata setup, selected-key collection, and referencing-row checks | `delete_data` |

UPDATE's earlier schema lookup and assignment setup remain outside the collection function.
DELETE's collection function includes its metadata setup. Both spans measure whole functions;
there is no attribute-defined start or end point and no automatically recorded batch count.

`fetch_rows` in `insert/schemaful.rs` and `insert/schemaless.rs` measures INSERT preparation. Both prepare insertion buffers before the storage write. The schemaful
function also performs uniqueness and foreign-key checks and prepares append or keyed-insert
data. The schemaless function converts input documents and collects the value rows.

`validate_unique` covers uniqueness validation. The primary-key-only path uses individual
lookups; other unique constraints can scan stored rows. Inspect its child storage method spans
and surrounding collection span to identify the path's costs. No loop counter is generated.

## Storage access and lazy work

`rows` prepares table access according to the query plan; `fetch` prepares mutation input.
The spans measure these functions without classifying access paths or recording row counts.

Storages opt into method spans. RedbStorage is the current reference implementation; its
method spans observe the actual operations in Redb's internal `StorageCore`
and require Redb tracing. The public trait methods delegate to those operations without adding
wrapper spans. Other storages need their own integration.
`observe` records method execution time without capturing arguments, errors, rows, or
batch sizes.

`scan_data` measures iterator creation and preparation. Its scope differs
between the two Redb scan paths:

| Scan path | Time measured by `scan_data` | Work during iterator consumption |
| --- | --- | --- |
| Normal read path | Read-transaction and table setup, plus range iterator creation and preparation | Row reads and decoding |
| Active explicit transaction (`autocommit=false`) | All row reads, decoding, and vector collection, plus iterator creation over the collected rows | Traversal of the collected rows |

Iterator consumption time is included in the consuming function's span, when that function is
instrumented. It is not measured separately by a storage iterator span. A short `scan_data` duration on the normal read path
therefore describes inexpensive iterator preparation, not necessarily inexpensive row reads.

## Interpreting measurements together

A long materialization span with a short DISTINCT span can include duplicate-check time during
iterator consumption. A long order-by span includes reading and collecting its input
as well as sorting. Use function boundaries to identify what each duration includes, then split
a cohesive operation into another observed function if the existing boundary is too broad.

For mutations, a short collection span followed by a long storage-write span points to work
after preparation. A long INSERT preparation span can include uniqueness checks rather than
just reading insertion values. Inspect nested validation spans before attributing the duration
to writes.

Align resource-benchmark RSS samples with function intervals to investigate memory growth.
An increase during sorting or hash-build preparation identifies overlapping work, not memory
allocated exclusively by that span. Parent and child intervals overlap and buffers can coexist.
Peak RSS is a process-lifetime high-water mark; run comparisons in fresh processes. See
[Resource benchmark profiles](#resource-benchmark-profiles) for sampling and viewing RSS.

## Data handling

Built-in function spans record names and timing without capturing SQL, arguments, access paths,
parameters, iterator rows, or returned errors. Custom subscribers and application events can
still contain application data; select fields and handle redaction, retention, and access
control at those integration points.
