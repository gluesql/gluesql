# Observability

GlueSQL tracing measures query execution at function boundaries. This guide covers enabling
tracing, viewing profiles, and interpreting measurements. See
[Adding observability](instrumentation.md) to instrument functions or storages.

## Enable tracing

Tracing is optional and disabled by default. Build the CLI with the outputs you need:

| Feature | Output |
| --- | --- |
| `tracing` | Formatted span-close events on standard error |
| `tracing-flame` | Formatted events and folded stacks for flamegraphs |
| `opentelemetry` | Formatted events and OTLP traces over HTTP/Protobuf |

Both exporter features enable `tracing` and can be used together. For example:

```sh
cargo build -p gluesql-cli --features tracing-flame,opentelemetry
```

Keep reusable CLI settings in a TOML file:

```toml
# gluesql.toml
[observability]
filter = "gluesql=debug"

[observability.flamegraph]
path = "query.folded"

[observability.otlp]
endpoint = "http://localhost:4318"
```

Select the file explicitly; it is not discovered automatically:

```sh
./target/debug/gluesql-cli --config gluesql.toml
```

Omit exporter sections that your CLI build does not support. Unknown keys, invalid TOML, and
invalid effective settings cause startup errors before storage is opened. Relative output paths
use the current working directory; their parent directories must already exist.

Each setting uses the first available value from CLI options, environment variables, TOML,
then the default:

| Setting | CLI option | Environment variable | TOML key | Default |
| --- | --- | --- | --- | --- |
| Filter | `--log-filter` | `RUST_LOG` | `observability.filter` | `gluesql=info` |
| Folded output | `--flamegraph-path` | `GLUESQL_FLAMEGRAPH_PATH` | `observability.flamegraph.path` | `tracing.folded` |
| OTLP destination | `--otlp-endpoint` | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, then `OTEL_EXPORTER_OTLP_ENDPOINT` | `observability.otlp.endpoint` | `http://localhost:4318` |

OTLP base URLs receive the `/v1/traces` suffix. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is a complete
trace URL and is used unchanged. Standard settings such as `OTEL_SERVICE_NAME` and headers
continue to use OpenTelemetry environment variables. The CLI flushes pending traces on exit.

Use the filter to select measurement detail:

| Level | Measurements under the `gluesql` target |
| --- | --- |
| `info` | Complete query execution and SQL source text |
| `debug` | Parsing, translation, planning, executor functions, and access-path fields |
| `trace` | Also method spans for storages with tracing integration |

For a temporary override, run `./target/debug/gluesql-cli --config gluesql.toml --log-filter gluesql=trace`.
The default in-memory session emits core spans; storage method spans require an integrated
backend such as RedbStorage. Run these queries to compare access paths:

```sql
CREATE TABLE items (id INTEGER PRIMARY KEY);
INSERT INTO items VALUES (1), (2);
SELECT * FROM items WHERE id = 1;
SELECT * FROM items;
```

The SELECTs record `access_path="primary_key"` and `access_path="full_scan"`, respectively.
Append `2> query.log` to the CLI command to save tracing output separately from query results.

### Using GlueSQL as a library

```toml
[dependencies]
gluesql = { version = "0.20", features = ["tracing"] }
```

`Glue::new` installs a formatted subscriber with span-close events and a `RUST_LOG` filter
when no dispatcher has already been configured. No initialization call is required. To use a
custom subscriber or exporter, install it before constructing `Glue`; GlueSQL preserves it.
CLI TOML settings do not apply to library applications.

## View profiles

For folded stacks, exit the CLI to flush output, then generate an SVG:

```sh
cargo install inferno
inferno-flamegraph < query.folded > query.svg
```

The CLI excludes empty samples so waiting at the interactive prompt does not dominate the
graph. `tracing-flame` measures elapsed time between span events, not sampled CPU usage.

The CLI's OpenTelemetry exporter sends completed spans to an HTTP/Protobuf collector, which
can forward them to Jaeger, Grafana Tempo, or another compatible backend. Library applications
configure their own exporter and subscriber; see the
[OpenTelemetry exporter](https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/),
[OpenTelemetry layer](https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/), and
[flame layer](https://docs.rs/tracing-flame/latest/tracing_flame/) documentation.

### Resource benchmark profiles

The RedbStorage example runs a SQL file once and records query spans, resource measurements,
and optional Firefox Profiler output. Generate a profile using a new database path:

```sh
GLUESQL_FIREFOX_PROFILE_PATH=/tmp/gluesql-profile.json \
RUST_LOG=gluesql=debug \
cargo run --release -p gluesql-redb-storage \
  --example resource_benchmark --features firefox-profile \
  -- /tmp/gluesql-benchmark.redb \
  storages/redb-storage/examples/resource_benchmark.sql
```

Open [Firefox Profiler](https://profiler.firefox.com/), select **Load a profile from file**, and
choose the JSON file. GlueSQL spans appear as interval markers and events as instant markers;
select the `process_rss` counter track to inspect memory over time. The profile stays local
unless you upload or share it.

| Measurement or setting | Meaning or default |
| --- | --- |
| `gluesql.benchmark.run` | Complete workload; `benchmark.name` is the SQL filename without its extension |
| `process.memory.peak_bytes` | Process-lifetime peak resident set size |
| `gluesql.database.size_bytes` | Persistent storage size after the workload, when measurable |
| `process.executable.size_bytes` | Benchmark executable size |
| `gluesql.benchmark.memory_sample` | Current RSS with `elapsed_ms` and `rss_bytes` |
| `GLUESQL_MEMORY_SAMPLE_MS` | Positive sampling interval in milliseconds; default `10` |
| `GLUESQL_FIREFOX_PROFILE_PATH` | Profile output; default `gluesql-benchmark-profile.json` |

Use `--features tracing` instead of `firefox-profile` for formatted output only. At `info`,
only final resource fields are recorded; `debug` and `trace` also enable RSS sampling.
Current RSS sampling supports macOS and Linux; peak RSS supports Unix. Samples include sampler
thread overhead. Compare fresh processes and database paths using the same build profile and
platform, because peak RSS is a process-lifetime high-water mark.

## Follow a query through its spans

`Glue::execute` and `execute_with_params` create `gluesql.execute` for the complete SQL string.
It includes parsing, parameter conversion, and translation, planning, and execution of each
statement. SQL text is recorded at entry; converted parameter values are not recorded.

Execution-layer spans use their function names, such as `execute`, `rows`, and `sort`.
Read them in their calling hierarchy; the function paths in the tables below identify the
corresponding implementation. Multiple operators can have the same function name, so the
name alone does not identify the operator.

An illustrative SELECT hierarchy with Redb tracing enabled is:

```text
gluesql.execute { sql }
├── gluesql.parse
├── gluesql.translate
├── gluesql.plan
└── gluesql.execute_statement
    ├── gluesql.RedbStorage.begin
    ├── execute
    │   ├── rows { access_path }
    │   │   └── gluesql.RedbStorage.scan_data
    │   ├── sort
    │   └── ...
    └── gluesql.RedbStorage.commit
```

The children depend on the query plan and storage integration. For a simple table query with
ORDER BY on Redb's normal read path, `gluesql.RedbStorage.scan_data` measures storage iterator
creation and preparation. `sort` measures input iterator consumption together
with sorting. Storage reads and decoding during consumption are included in the order-by span;
they do not create or extend a storage span.

Parsing happens once per SQL string. Translation, planning, and execution repeat per statement,
so later statements see changes made by earlier statements. A separate call to `Glue::plan` or
`plan_with_params` uses `gluesql.plan` for the complete planning call, including parsing and
translation. Within an execute trace, that name wraps only the per-statement storage planner.

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

UPDATE and DELETE use the `collect_update_rows` and `collect_keys` function spans.
The entry field `operation` also identifies the mutation:

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

`rows` records the planned `access_path` at function entry:

| Value | Meaning |
| --- | --- |
| `primary_key` | Primary-key lookup |
| `secondary_index` | Secondary-index access |
| `full_scan` | Scan without a key or index access path |

This describes the plan selected for the call, including calls that fail while evaluating a key
or preparing access. `fetch` also identifies its full-scan path at entry. Neither
span reports the number of visited rows.

Storages opt into method spans. RedbStorage is the current reference implementation; its
`gluesql.RedbStorage.*` spans require Redb tracing. Other storages need their own integration.
`trace_storage` records method execution time without capturing arguments, errors, rows, or
batch sizes.

`gluesql.RedbStorage.scan_data` measures iterator creation and preparation. Its scope differs
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

Top-level execute and public plan spans record SQL source text. Custom entry fields can also
contain application data. Arguments, bound parameters, iterator rows, and returned errors are
not captured automatically. Applications are responsible for selecting fields and handling
redaction, retention, and access control in their subscriber or exporter.
