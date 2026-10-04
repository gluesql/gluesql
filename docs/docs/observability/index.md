# Execution stages and interpretation

GlueSQL tracing identifies where query execution spends time. Start here to understand the
existing function spans. Use [Tools and setup](tools.md) to enable tracing or load a profile,
and [Adding observability](instrumentation.md) to instrument functions and storages.

Tracing is optional and disabled by default. Core uses the `gluesql` target. INFO exposes the
complete execution span, DEBUG adds execution-stage spans, and TRACE adds method spans for
integrated storages. Automatic loop, buffer, return-value, error, and iterator counters are not
part of these observations.

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

The attribute is a whole-function declaration:

```rust
#[cfg_attr(feature = "tracing", gluesql_macros::observe)]
```

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

`fetch_rows` surrounds `fetch_rows` in `insert/schemaful.rs` and
`insert/schemaless.rs`. Both prepare insertion buffers before the storage write. The schemaful
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
instrumented. It is not measured separately by a storage iterator span. There are no `scan_rows`
spans or iterator completion counters. A short `scan_data` duration on the normal read path
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
[Resource benchmark profiles](tools.md#resource-benchmark-profiles) and
[Firefox Profiler output](tools.md#firefox-profiler-output) for sampling and viewing RSS.

## Data handling

Top-level execute and public plan spans record SQL source text. Custom entry fields can also
contain application data. Arguments, bound parameters, iterator rows, and returned errors are
not captured automatically. Applications are responsible for selecting fields and handling
redaction, retention, and access control in their subscriber or exporter.
