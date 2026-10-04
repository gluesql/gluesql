# Tools and setup

Use this guide to enable tracing and view observations in console logs, Firefox Profiler,
OpenTelemetry, or flamegraphs. For the meaning and scope of the measurements, see
[Execution stages and interpretation](index.md). To add observation points, see
[Adding observability](instrumentation.md).

GlueSQL can emit structured spans and events for query execution when the optional `tracing`
feature is enabled. The feature is disabled by default, so applications that do not use
observability do not compile the instrumentation or its dependency.

When `tracing` is enabled, `Glue::new` installs a formatted default subscriber if the process has
not already configured one. Applications can install their own `tracing-subscriber`, OpenTelemetry,
or `tracing-flame` subscriber before constructing `Glue`; GlueSQL detects and preserves it. The
GlueSQL CLI configures its exporter layers before constructing `Glue`.

## CLI logging

Install or build the CLI with tracing enabled:

```sh
cargo install gluesql --features tracing
```

Set `RUST_LOG` to select the required detail:

```sh
RUST_LOG=gluesql=info gluesql
RUST_LOG=gluesql=debug gluesql
RUST_LOG=gluesql=trace gluesql
```

The levels have the following intended scope:

| Level | Data |
| --- | --- |
| `info` | Total query execution time, SQL source text, and bound parameters |
| `debug` | Parse, translate, plan, statement execution, access paths, and execution-stage counts |
| `trace` | Transaction, primary storage, and enabled backend call boundaries |

The CLI reports span close events, including busy and idle durations.

Optional CLI exporter features build on the same instrumentation:

| Feature | Output |
| --- | --- |
| `tracing` | Formatted span close events on standard error |
| `tracing-flame` | Formatted events and folded stack data |
| `opentelemetry` | Formatted events and OTLP traces over HTTP/Protobuf |

`tracing-flame` and `opentelemetry` both enable `tracing` and can be enabled together.

### TOML configuration

Keep reusable CLI settings in a TOML file and select it explicitly with `--config`:

```toml
# gluesql.toml
[observability]
filter = "gluesql=debug"

[observability.flamegraph]
path = "query.folded"

[observability.otlp]
endpoint = "http://localhost:4318"
```

Build with the exporters used by the file, then run the CLI:

```sh
cargo build -p gluesql-cli --features tracing-flame,opentelemetry
./target/debug/gluesql-cli --config gluesql.toml
```

The file is not discovered automatically. Omit exporter sections that the CLI build does not
support; specifying them produces a startup error. Unknown keys, invalid TOML, and invalid
effective settings also produce errors before storage is opened. Relative output paths are
resolved against the current working directory, and their parent directories must already exist.

Each setting uses the first available value in this order:

| Setting | CLI option | Environment variable | TOML key | Default |
| --- | --- | --- | --- | --- |
| Span/event filter | `--log-filter` | `RUST_LOG` | `observability.filter` | `gluesql=info` |
| Folded stack output | `--flamegraph-path` | `GLUESQL_FLAMEGRAPH_PATH` | `observability.flamegraph.path` | `tracing.folded` |
| OTLP HTTP destination | `--otlp-endpoint` | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, then `OTEL_EXPORTER_OTLP_ENDPOINT` | `observability.otlp.endpoint` | `http://localhost:4318` |

The TOML endpoint, CLI endpoint, and general `OTEL_EXPORTER_OTLP_ENDPOINT` are collector base
URLs: `/v1/traces` is appended. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` specifies the complete trace
URL and is used unchanged. Other OpenTelemetry settings, such as `OTEL_SERVICE_NAME` and headers,
continue to use the standard environment variables.

For example, temporarily increase the detail without editing the file:

```sh
./target/debug/gluesql-cli --config gluesql.toml --log-filter gluesql=trace
```

This configuration applies to the CLI. Applications using GlueSQL as a library continue to
configure their own subscriber; `Glue::new` does not load this file. The resource benchmark's
memory sampling and Firefox Profiler settings remain specific to that example.

### Try tracing locally

From the repository root, build the CLI with tracing enabled:

```sh
cargo build -p gluesql-cli --features tracing
```

Start the CLI with its default in-memory storage and trace-level logging:

```sh
RUST_LOG=gluesql=trace \
./target/debug/gluesql-cli
```

Run these statements at the `gluesql>` prompt:

```sql
CREATE TABLE Items (
    id INTEGER PRIMARY KEY,
    name TEXT
);

INSERT INTO Items VALUES
    (1, 'apple'),
    (2, 'banana'),
    (3, 'cherry');

SELECT * FROM Items WHERE id = 1;
SELECT * FROM Items;
```

The query with the primary-key predicate emits an access-path event with
`access_path="primary_key"`. The query without a predicate emits `access_path="full_scan"`.
Storage method spans require a storage with tracing integration; RedbStorage is the current
reference implementation. The default in-memory CLI session still emits core execution spans
and access-path events.

Tracing output is written to standard error. Redirect it to a file while keeping query results in
the terminal:

```sh
RUST_LOG=gluesql=trace \
./target/debug/gluesql-cli 2> ~/gluesql-trace.log
```

Follow the trace from another terminal:

```sh
tail -f ~/gluesql-trace.log
```

## Library logging

Enable GlueSQL instrumentation:

```toml
[dependencies]
gluesql = { version = "0.20", features = ["tracing"] }
```

No initialization call is required. `Glue::new` reads `RUST_LOG` and installs a formatted
subscriber with span-close events when no dispatcher has previously been set:

```rust
let mut glue = Glue::new(storage);
glue.execute("SELECT * FROM Items")?;
```

To use a custom subscriber, install it before constructing `Glue`:

```toml
[dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

```rust
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

let filter = EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| EnvFilter::new("gluesql=info"));

tracing_subscriber::fmt()
    .with_env_filter(filter)
    .with_span_events(FmtSpan::CLOSE)
    .init();
```

The existing subscriber then receives GlueSQL spans under the `gluesql` target, and GlueSQL does
not replace it.

## Resource benchmark profiles

A resource benchmark groups one SQL workload, query spans, and resource measurements under a
`gluesql.benchmark.run` span. The field names and Firefox Profiler representation below are shared
across storage implementations. The current runnable reference is in the RedbStorage crate; it is
not the definition of the generic storage tracing contract.

Execute the SQL file once and use its filename without the extension as `benchmark.name`. The
closing benchmark span uses these storage-independent fields:

| Field | Meaning |
| --- | --- |
| `process.memory.peak_bytes` | Peak resident set size of the benchmark process |
| `gluesql.database.size_bytes` | Storage-owned persistent data size after the workload, when measurable |
| `process.executable.size_bytes` | Benchmark executable file size |

At `debug` or `trace` level, a benchmark can also emit current RSS samples under the run span:

```text
gluesql.benchmark.memory_sample elapsed_ms=20 rss_bytes=18874368
```

### Run the RedbStorage reference

The reference example is available only when its `tracing` feature is enabled.

Run a SQL workload against a new Redb database path:

```sh
RUST_LOG=gluesql=trace \
cargo run --release \
  -p gluesql-redb-storage \
  --example resource_benchmark \
  --features tracing \
  -- /tmp/gluesql-benchmark.redb \
  storages/redb-storage/examples/resource_benchmark.sql
```

The default interval is 10 milliseconds. Set `GLUESQL_MEMORY_SAMPLE_MS` to use a different
positive interval:

```sh
GLUESQL_MEMORY_SAMPLE_MS=50 \
RUST_LOG=gluesql=debug \
cargo run --release \
  -p gluesql-redb-storage \
  --example resource_benchmark \
  --features tracing \
  -- /tmp/gluesql-benchmark.redb \
  storages/redb-storage/examples/resource_benchmark.sql
```

The example emits tracing data only; it does not select a graphing or storage format. Subscribers
can consume `elapsed_ms` and `rss_bytes` to produce a step chart and align it with the existing
query spans. At `info` level the sampler is not started, so only the final resource fields are
recorded. RSS samples include the memory and scheduling overhead of the sampler thread itself.
Current RSS sampling is supported on macOS and Linux.

### Firefox Profiler output

The optional `firefox-profile` feature keeps the formatted standard-error output and also writes
GlueSQL spans, events, and RSS samples directly in the Firefox Profiler processed-profile JSON
format.

Generate a profile from one workload:

```sh
GLUESQL_FIREFOX_PROFILE_PATH=~/gluesql-benchmark-profile.json \
GLUESQL_MEMORY_SAMPLE_MS=10 \
RUST_LOG=gluesql=debug \
cargo run --release \
  -p gluesql-redb-storage \
  --example resource_benchmark \
  --features firefox-profile \
  -- /tmp/gluesql-benchmark.redb \
  storages/redb-storage/examples/resource_benchmark.sql
```

`GLUESQL_FIREFOX_PROFILE_PATH` defaults to `gluesql-benchmark-profile.json`. Open
[Firefox Profiler](https://profiler.firefox.com/), select **Load a profile from file**, and choose
the generated JSON file. Select the `process_rss` counter track to inspect RSS over time. GlueSQL
spans and events appear as interval and instant markers on the same timeline. The profile remains
local unless it is explicitly uploaded or shared; GlueSQL does not require a visualization
service.

Peak RSS is a process-lifetime high-water mark, so run each workload in a separate process and use
a new database path when comparing results. Use the same build profile and target platform for
executable-size comparisons. Peak RSS measurement is currently supported on Unix platforms.

## OpenTelemetry

OpenTelemetry integration belongs to the host application rather than `gluesql-core`. Add the
OpenTelemetry crates that match the application's chosen transport:

### CLI OTLP export

Build the CLI with the OpenTelemetry exporter:

```sh
cargo build -p gluesql-cli --features opentelemetry
```

Set the standard OpenTelemetry environment variables and run the CLI:

```sh
OTEL_SERVICE_NAME=gluesql-cli \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
RUST_LOG=gluesql=trace \
./target/debug/gluesql-cli
```

The CLI exports completed spans to `/v1/traces` and flushes pending batches when it exits. The
configured endpoint must accept OTLP over HTTP/Protobuf. A collector can forward those traces to
Jaeger, Grafana Tempo, or another compatible backend.

### Application integration

```sh
cargo add tracing-opentelemetry opentelemetry opentelemetry_sdk
cargo add opentelemetry-otlp --features grpc-tonic
```

Create an OTLP exporter and attach the OpenTelemetry layer to the application's subscriber:

```rust
use {
    opentelemetry::trace::TracerProvider as _,
    opentelemetry_sdk::trace::SdkTracerProvider,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt},
};

let exporter = opentelemetry_otlp::SpanExporter::builder()
    .with_tonic()
    .build()?;
let provider = SdkTracerProvider::builder()
    .with_batch_exporter(exporter)
    .build();
let tracer = provider.tracer("gluesql");
let filter = EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| EnvFilter::new("gluesql=info"));

tracing_subscriber::registry()
    .with(filter)
    .with(tracing_opentelemetry::layer().with_tracer(tracer))
    .init();

// Run GlueSQL queries here.

provider.shutdown()?;
```

Configure the collector endpoint with the standard OpenTelemetry environment variables:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

The collector can forward traces to Jaeger, Grafana Tempo, or another OTLP-compatible backend. See
the
[`opentelemetry-otlp` exporter documentation](https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/)
and
[`tracing-opentelemetry` layer documentation](https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/)
for transport and SDK-specific configuration.

## Flamegraphs

`tracing-flame` can convert the same span hierarchy into folded stack data:

### CLI flamegraph

Build the CLI with the flame exporter:

```sh
cargo build -p gluesql-cli --features tracing-flame
```

Run a workload and choose the folded output path with `GLUESQL_FLAMEGRAPH_PATH`. The default path
is `tracing.folded`.

```sh
GLUESQL_FLAMEGRAPH_PATH=~/gluesql.folded \
RUST_LOG=gluesql=trace \
./target/debug/gluesql-cli
```

After exiting the CLI, generate an SVG with Inferno:

```sh
cargo install inferno
```

```sh
inferno-flamegraph < ~/gluesql.folded > ~/gluesql.svg
```

The CLI keeps empty samples out of the folded output so time waiting at the interactive prompt
does not dominate the graph.

### Application integration

```rust
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

let (flame_layer, _guard) = tracing_flame::FlameLayer::with_file("tracing.folded")?;
let filter = EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| EnvFilter::new("gluesql=info"));

tracing_subscriber::registry()
    .with(filter)
    .with(flame_layer)
    .init();
```

Keep the returned guard alive until tracing has finished so buffered output is flushed. Generate
an SVG with Inferno:

```sh
inferno-flamegraph < tracing.folded > tracing.svg
```

`tracing-flame` measures elapsed time between instrumented span events; it is not a sampling CPU
profiler. Use `perf` or `cargo-flamegraph` when function-level CPU samples are required.
