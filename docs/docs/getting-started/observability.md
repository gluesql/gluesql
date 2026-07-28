---
sidebar_position: 5
---

# Observability

GlueSQL can emit structured spans and events for query execution when the optional `tracing`
feature is enabled. The feature is disabled by default, so applications that do not use
observability do not compile the instrumentation or its dependency.

GlueSQL does not install a global subscriber when used as a library. Applications can connect the
emitted data to `tracing-subscriber`, OpenTelemetry, or `tracing-flame`. The GlueSQL CLI installs a
formatted subscriber automatically when it is built with the feature.

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
| `info` | Total query execution time |
| `debug` | Parse, translate, plan, statement execution, and selected access path |
| `trace` | Transaction, primary storage, and enabled backend call boundaries |

The CLI reports span close events, including busy and idle durations.

Optional CLI exporter features build on the same instrumentation:

| Feature | Output |
| --- | --- |
| `tracing` | Formatted span close events on standard error |
| `tracing-flame` | Formatted events and folded stack data |
| `opentelemetry` | Formatted events and OTLP traces over HTTP/Protobuf |

`tracing-flame` and `opentelemetry` both enable `tracing` and can be enabled together.

### Try Redb tracing locally

From the repository root, build the CLI with tracing enabled:

```sh
cargo build -p gluesql-cli --features tracing
```

Start the CLI with RedbStorage and trace-level logging. Use a new database path if the example has
already been run:

```sh
RUST_LOG=gluesql=trace \
./target/debug/gluesql-cli \
  --storage redb \
  --path /tmp/gluesql-tracing-demo.redb
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

The query with the primary-key predicate emits `gluesql.storage.fetch_data` and
`gluesql.redb.fetch_data`. The query without a predicate uses a full scan and emits
`gluesql.storage.scan_data`, `gluesql.redb.scan_data`, and
`gluesql.redb.scan_rows{row_count=3}`.

Tracing output is written to standard error. Redirect it to a file while keeping query results in
the terminal:

```sh
RUST_LOG=gluesql=trace \
./target/debug/gluesql-cli \
  --storage redb \
  --path /tmp/gluesql-tracing-demo.redb \
  2> /tmp/gluesql-trace.log
```

Follow the trace from another terminal:

```sh
tail -f /tmp/gluesql-trace.log
```

For `gluesql.redb.scan_rows`, `time.busy` is time spent reading and deserializing rows,
`time.idle` is time spent by the consumer between iterator reads, and `row_count` is the number of
items yielded by the iterator.

## Library logging

Enable GlueSQL instrumentation and add a subscriber in the application:

```toml
[dependencies]
gluesql = { version = "0.19", features = ["tracing"] }
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

Install the subscriber once, before executing queries:

```rust
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

let filter = EnvFilter::try_from_default_env()
    .unwrap_or_else(|_| EnvFilter::new("gluesql=info"));

tracing_subscriber::fmt()
    .with_env_filter(filter)
    .with_span_events(FmtSpan::CLOSE)
    .init();
```

Libraries should not install their own global subscriber. If the host application already uses
`tracing`, enabling GlueSQL's feature is sufficient; the existing subscriber receives GlueSQL
spans under the `gluesql` target.

## Span hierarchy

The initial instrumentation follows the query execution pipeline:

```text
gluesql.execute
├── gluesql.parse
├── gluesql.translate
├── gluesql.plan
└── gluesql.execute_statement
    ├── gluesql.storage.begin
    │   └── gluesql.redb.begin
    ├── gluesql.storage.fetch_data
    │   └── gluesql.redb.fetch_data
    ├── gluesql.storage.scan_data
    │   └── gluesql.redb.scan_data
    ├── gluesql.redb.scan_rows
    ├── gluesql.storage.scan_indexed_data
    ├── gluesql.storage.commit
    │   └── gluesql.redb.commit
    └── gluesql.storage.rollback
        └── gluesql.redb.rollback
```

The `gluesql.redb.*` spans are emitted when RedbStorage and its `tracing` feature are enabled. The
top-level `gluesql` and CLI `tracing` features enable Redb instrumentation when they include
RedbStorage.

Access-path events use one of these stable values:

```text
primary_key
secondary_index
full_scan
```

`scan_data` and `scan_indexed_data` return lazy iterators. Their generic storage spans measure
iterator creation; `gluesql.execute_statement` includes subsequent iterator consumption.
RedbStorage additionally emits `gluesql.redb.scan_rows` from the first iterator read until the
iterator is dropped. Its busy duration measures Redb row reads and deserialization, its idle
duration covers time spent by the consumer between reads, and its `row_count` field records the
number of yielded items.

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
./target/debug/gluesql-cli \
  --storage redb \
  --path /tmp/gluesql-tracing-demo.redb
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
GLUESQL_FLAMEGRAPH_PATH=/tmp/gluesql.folded \
RUST_LOG=gluesql=trace \
./target/debug/gluesql-cli \
  --storage redb \
  --path /tmp/gluesql-tracing-demo.redb
```

After exiting the CLI, generate an SVG with Inferno:

```sh
inferno-flamegraph < /tmp/gluesql.folded > /tmp/gluesql.svg
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

## Data handling

GlueSQL does not record the following values in its default instrumentation:

- SQL source text
- Bound parameters
- Row contents
- Full error messages

The default fields are limited to stable execution metadata such as the access path and
transaction mode. Applications that add their own fields or layers are responsible for applying
their data retention and access-control policies.
