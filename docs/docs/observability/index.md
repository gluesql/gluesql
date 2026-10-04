# Execution stages and interpretation

GlueSQL tracing identifies where query execution spends time and how much data each stage
processes or retains. Start with this guide to understand the existing observations. Use
[Tools and setup](tools.md) to enable tracing, view logs, or load a profile, and
[Adding observability](instrumentation.md) to instrument functions and storages.

Tracing is optional and disabled by default. Core execution stages use the `gluesql` target;
INFO exposes the overall execution span, DEBUG adds stage spans and access paths, and TRACE
adds storage calls and iterator consumption for integrated storages.

## Follow a query through its spans

`Glue::execute` and `execute_with_params` create a `gluesql.execute` span for the complete SQL
string. The span includes parsing, parameter conversion, and the translation, planning, and
execution of each statement. SQL text is captured at entry; converted parameters are recorded
after conversion.

The following is an illustrative SELECT hierarchy with Redb tracing enabled. The actual children
depend on the query plan and storage integration:

```text
gluesql.execute { sql, params }
├── gluesql.parse
├── gluesql.translate
├── gluesql.plan
└── gluesql.execute_statement
    ├── gluesql.redb.begin
    ├── gluesql.result.materialize
    │   ├── gluesql.redb.scan_data
    │   │   └── gluesql.redb.scan_rows
    │   ├── gluesql.query.order_by
    │   └── ...
    └── gluesql.redb.commit
```

For a simple table query with `ORDER BY`, the storage iterator is created before the sorting
span starts. `scan_data` and `order_by` are therefore siblings under `result.materialize`.
Although sorting consumes the iterator, `scan_rows` retains `scan_data` as its parent; consuming
an iterator inside another span does not change its parent relationship.

For a multi-statement script, parsing happens once and translation, planning, and execution
repeat per statement. Planning after each previous execution lets later statements see catalog
changes. A separate call to `Glue::plan` or `plan_with_params` uses `gluesql.plan` for the whole
planning call, including parsing and translation; the same span name in an execute trace wraps
only the per-statement storage planner call.

Span timings include work performed by child operations. Do not add a parent duration to its
child durations to calculate total query time. A field recording point also does not delimit a
timing interval: a function may record its buffer size halfway through and continue executing
inside the same span.

## SELECT: producing the final result

`gluesql.result.materialize` surrounds `core/src/executor/select.rs::execute`. This function
prepares the query output, consumes its iterator, converts each row into the public result
representation, and collects the result into a `Vec`.

On success, `buffered_rows` is the number of rows in the returned payload. Both regular
`Payload::Select` rows and schemaless `Payload::SelectMap` documents are counted. For a query
that reads many rows but returns ten, this field is ten; it is not the number of rows read from
the storage. A successful empty result records zero. If execution or collection fails, there is
no successful payload and this field is not recorded.

The observation is declared on the function rather than inside its two result branches:

```rust
#[cfg_attr(
    feature = "tracing",
    gluesql_macros::observe(
        name = "gluesql.result.materialize",
        target = "gluesql",
        level = "debug",
        on_ok(payload, record(buffered_rows = match payload {
            Payload::Select { rows, .. } => rows.len(),
            Payload::SelectMap(rows) => rows.len(),
            _ => unreachable!("select executor returned a non-select payload"),
        }))
    )
)]
```

This span covers the entire SELECT executor function, including nested query operations. Its
duration is not just the cost of appending rows to the final vector. Use the child stage spans
to identify sorting, aggregation, or storage work contributing to that duration.

## Query operators: preparing intermediate data

Operators can retain much more data than the final query returns. Their counts describe
particular intermediate buffers, not interchangeable totals:

| Span | Function | Field and recording point |
| --- | --- | --- |
| `gluesql.query.order_by` | `query/order_by/select.rs::sort` | `buffered_rows`: input rows collected before computing sort keys and sorting |
| `gluesql.query.distinct` | `query/distinct.rs::execute` | `buffered_rows`: input rows collected before duplicate filtering |
| `gluesql.query.hash_join.build` | `query/join/hash.rs::build_rows` | `buffered_rows`: build-side rows retained after the collection loop, before grouping by join key |
| `gluesql.query.aggregate` | `query/aggregation.rs::execute` | `buffered_groups`: groups exported after processing the input |

Function paths in this table are relative to `core/src/executor/`.

### Sorting

The order-by span covers input collection, sort-key evaluation, sorting, and preparation of the
returned iterator. The count is recorded as soon as input collection succeeds. If later key
evaluation fails, that count can still be present. When the function is called without ordering
expressions, it returns an iterator without collecting rows and does not record this field.

### Duplicate removal

The distinct span covers preparing its input, collecting all input rows, and constructing a
lazy duplicate filter. The recorded count is before deduplication. The actual duplicate checks
run when the returned iterator is consumed, after the distinct function span has closed. That
work can therefore appear inside the enclosing result-materialization span instead.

### Hash-join preparation

The hash-build span covers reading the build input, evaluating join keys and filters, collecting
retained rows, and grouping them into the lookup map. Rows with null join keys or failing the
build-side filter are not retained and are not included in `buffered_rows`. This is neither the
number of all rows read nor the number of final joined rows. The count is recorded only after
the collection loop finishes; a failure during the loop leaves it unrecorded.

### Aggregation

The aggregate span covers preparing and consuming the input, grouping, accumulating aggregate
values, and exporting the groups. `buffered_groups` counts exported groups, not input rows or
the number of retained bytes. Many input rows may produce few groups, while high-cardinality
grouping can produce many groups. The final group count is absent if processing or export fails.

## Mutations: collecting data before writing

UPDATE and DELETE use a partial-function `gluesql.mutation.collect` span. The `operation` field
distinguishes the two paths. Their start and end points are deliberately before the storage
mutation:

| Operation | Included work | Recorded count | Work outside the interval |
| --- | --- | --- | --- |
| UPDATE | Fetching selected rows and applying assignments while collecting modified rows | `buffered_rows`: collected modified rows | Earlier schema and update setup; subsequent uniqueness validation, write-batch conversion, and `insert_data` |
| DELETE | Collecting selected keys and checking referencing rows | `buffered_rows`: collected deletion keys | Earlier column and referencing-metadata setup; subsequent `delete_data` |

Use these spans to distinguish preparing a mutation from committing its writes. If collection
fails before the endpoint, the span closes without its final count. A later storage-write
failure does not invalidate a count already recorded for successful collection.

### INSERT preparation

`gluesql.insert.collect` surrounds the `fetch_rows` functions in
`executor/insert/schemaful.rs` and `executor/insert/schemaless.rs`, relative to `core/src/`.
Both produce insertion buffers from VALUES or query results; storage writes happen later.

The schemaful path records `buffered_rows` immediately after collecting the value rows. Its
function span also includes uniqueness and foreign-key checks and preparation of append or
keyed-insert data. A later validation error can leave the collected count recorded.

The schemaless path converts each input to a document row and collects the rows. It records the
count from the successful returned vector, so a conversion or collection error leaves the field
absent. Although these paths use the same span name, their field-recording points differ.

### Uniqueness validation

`gluesql.validate.unique` surrounds `executor/validate.rs::validate_unique`. In the full-scan
path, `scanned_rows` increases after a storage item is successfully unpacked into row values.
An unreadable item is not counted; a readable row that reveals a duplicate is counted. The
counter is retained on error, so it shows how far validation progressed.

The primary-key-only path uses individual key lookups instead of this scan loop. It does not
record `scanned_rows`. A missing field therefore does not necessarily mean that no validation
was performed.

## Storage access and lazy consumption

Access-path events describe how the query accesses a table:

| `access_path` | Meaning |
| --- | --- |
| `primary_key` | Lookup using the evaluated primary key |
| `secondary_index` | Access using a secondary index |
| `full_scan` | Scan without a key or index access path |

An access-path event identifies the selected path, not the number of rows it visits. It is
emitted at DEBUG level. Inspect storage spans and stage counts for the work that follows.

Storages opt into method observations; RedbStorage is the current reference implementation.
Its `gluesql.redb.*` spans appear only with Redb tracing enabled. Core continues to invoke
storage traits directly, so other storages need their own integration.

A scan method such as `scan_data` returns a lazy iterator. Its method span measures iterator
creation. A child `scan_rows` span stays alive with the returned iterator and is entered for each
`next()` call, measuring consumption separately from construction. Indexed and metadata scans
use `scan_indexed_rows` and `scan_table_meta_rows` in storages that instrument those traits.

For the formatted subscriber, iterator busy time includes reads, decoding, and any enabled row
event handling during `next()`. Idle time covers periods while that span is not entered,
including time spent by the consumer between reads. Neither value is a CPU-utilization metric.

The iterator records these fields when dropped:

| Field | Meaning |
| --- | --- |
| `row_count` | Number of successfully yielded rows |
| `error_count` | Number of yielded error items |
| `completed` | Whether a `next()` call reached `None` |

A consumer can stop early, leaving `completed=false`. Even a consumer that reads exactly all
rows without one more `next()` call cannot establish completion for this wrapper. Counts then
describe observed consumption, not the total number of available rows. Iterator errors may be
yielded before later rows; `completed=true` does not imply `error_count=0`.

Batch-write method spans also use `row_count`, but there it is the number of supplied rows or
keys at method entry. It is not a count of completed writes. Full capture emits row and error
values as events; `capture = "off"` keeps timing and counts without these values.

## Interpreting measurements together

The following cases illustrate how to narrow an investigation. They describe possible observed
values, not measurements from the small example workload.

### Small final result, large intermediate buffer

Suppose a DISTINCT query records 100,000 input rows in `gluesql.query.distinct` but only 100 rows
in `gluesql.result.materialize`. The small result does not imply small intermediate work: all
100,000 input rows were buffered before duplicate filtering. Investigate the input size and
that collection stage, then examine the enclosing materialization interval for lazy duplicate
checks. A long materialization span alone does not isolate the cost of its final vector.

For an ORDER BY query, compare the sorting buffer count with the returned row count and inspect
the order-by child duration. This distinguishes sorting a large input from returning a large
result, without assuming which stage dominates from the counts alone.

### Expensive validation before a write

If an INSERT's uniqueness span is long and `scanned_rows` is large, many existing rows were
checked before the write. Inspect that validation path and its storage scans before attributing
the whole INSERT duration to storage writes. If a duplicate error ends the scan, the retained
counter shows the number of readable rows checked up to that error.

For UPDATE or DELETE, a short collection span followed by a long storage-write span points to
work after collection. The collection count describes the prepared batch even when a subsequent
write fails. A long DELETE collection span can also include referencing-row checks, not just
reading the deletion targets.

### Memory growth during a stage

Align current RSS samples with the stage intervals in a resource benchmark. An increase during
sorting or hash-build preparation identifies overlapping work worth examining, but is not an
allocation measurement for that span. Parent and child intervals overlap, and multiple buffers
can coexist. `buffered_rows` and `buffered_groups` count logical items; their sizes vary with
row values and aggregate state. Do not add these counts to calculate process memory.

Peak RSS is a process-lifetime high-water mark, not the peak for each individual span. Use a
fresh process for comparisons. See [Resource benchmark profiles](tools.md#resource-benchmark-profiles)
for running the sampler and [Firefox Profiler output](tools.md#firefox-profiler-output) for viewing
its RSS counter beside the intervals.

## Data handling

Full tracing deliberately records query and storage values that may contain sensitive data:

- Top-level `gluesql.execute` and public `gluesql.plan` spans record SQL source text and bound
  parameters.
- `trace_storage(capture = "full")` records simple named method arguments, including keys,
  schemas, and rows, together with `Result` errors.
- `trace_storage(capture = "full")` emits an event for every yielded row or error from traced iterators.

Enable tracing only in environments where this data is acceptable. Use `capture = "off"` when a
storage needs timing and iterator counts without argument, row, or error values, and use an
appropriate `RUST_LOG` filter to limit event volume. Applications are responsible for redaction,
retention, and access-control policies in their selected subscriber or exporter.
