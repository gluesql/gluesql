# Errors

## Error ownership

Use the shared storage-interface errors described below for their matching
failure conditions. For other errors, follow the declaration-location rule.

For an error created in a module, search that module and then its parents.
Use the first existing error type declared along that path, adding a variant
there if needed. A module's own `error.rs` counts as its declaration site;
imports and re-exports do not.

Choose by declaration location, regardless of feature, SQL statement, or
user-facing message. Reapply this rule when moving a check to another module.

An error propagated from a call keeps the type chosen where it was created.
The caller's location does not change its ownership.

Ownership selects the error type. Variants distinguish failures; prefixes
classify their conditions.

### Shared storage-interface errors

`AlterTableError` and `IndexError` are shared by implementations of the
corresponding storage traits. Use their existing variants for matching failure
conditions, including in new storage implementations. Preserve these variants
when returning GlueSQL's `Error`.

Use the declaration-location rule for storage-specific failures. Do not create
a storage-local duplicate of a shared error solely to satisfy that rule.
Reusing an existing error does not require reproducing another storage's
internal checks.

## Error variants

Give independent failure conditions distinct variants, even when their messages
would be similar. Do not reuse a broad variant across unrelated checks.

Represent distinctions within a variant with enum payloads, not string tags or
message text. String fields may carry diagnostic data such as table names.

For example, use `CreateIndexTableNotFound(String)` for a missing CREATE INDEX
target and `CtasSourceTableNotFound(String)` for a missing CTAS source. Each string
holds the table name; the variants identify the failed checks.

An error produced by the same check in a shared function may be propagated
unchanged by multiple callers.

## Error prefixes

The following error-variant prefixes have specific meanings. Choose a prefix by
examining the condition that produces the error and how that condition can be
reached.

| Prefix | Meaning |
| --- | --- |
| `Unreachable` | The branch cannot be reached under the conditions established by the current code. |
| `Conflict` | Storage state violates an invariant preserved by normal executor operations. Direct storage manipulation can make the error reachable. |
| `Unplanned` | Planning required before execution has not been completed. |
| `MalformedPlan` | An execution plan has an invalid structure or internal reference. Direct plan construction or modification can make the error reachable. |

Ordinary error names such as `InvalidSortOrder` and `InvalidJsonString` describe
invalid values or formats. `Invalid` does not have a special classification
meaning and is not reserved for malformed plans.

### Unreachable

Use `Unreachable` only when the current code makes the error branch impossible.
For example:

```rust
let values = [1, 2, 3];
let first = values.get(0);
```

Here, `first` is always `Some`. A branch handling `None` would be unreachable
because both the array length and the index are fixed by the code.

The claim must hold for the inputs the code accepts. A branch that can be reached
by constructing an AST or plan directly, or by changing storage contents, does
not qualify merely because normal SQL execution never reaches it. A planner
guarantee alone is insufficient when callers can supply their own plans.

During review, identify the code conditions that make the branch impossible. An
untested branch is not evidence of unreachability.

### Conflict

Use `Conflict` when stored state violates an invariant that normal executor
operations preserve. For example, a stored row can have fewer values than its
schema requires if a caller writes it directly through the storage interface.
Reading a missing column during validation can then produce
`ConflictOnStorageColumnIndex`.

Test this kind of guard by constructing the inconsistent storage state and
executing the operation that detects it. Assert the returned error and its
details. A normal SQL statement rejected before it changes storage does not
exercise that guard.

### Unplanned

`Unplanned` also names plan states that still need processing by a responsible
planner. AST conversion or direct plan construction can produce these states.
The planner consumes them and produces the representation needed for execution.

For custom function calls, the transition is:

```text
UnplannedCustom { name, exprs }
    -- custom function planner -->
Custom { function_id, exprs }
```

If an unplanned state reaches the executor, return an `Unplanned...` error with
the information needed to identify the unfinished work. For example,
`UnplannedCustomFunction` retains the function name. The executor must not perform
the missing planning step or make the state executable through a fallback.

Test the executor guard with an unplanned value that has not passed through its
required planner. Also check that the supported planning path consumes that
state before execution.

### MalformedPlan

Use `MalformedPlan` when a plan uses an execution representation but violates its
structural requirements. For example, a `Custom` call with `function_id: 10`
cannot refer to a definition when the statement's function list is empty. The
guard returns `MalformedPlanCustomFunctionIndex(10)`.

This differs from `UnplannedCustom`: the call already has an execution index,
but the index does not identify a prepared function. Callers can create this
state by constructing or modifying a plan. A planner bug can also produce it;
the error class describes the invalid plan without assuming its origin.

Test the guard by constructing the malformed plan and passing it to the relevant
execution path. Assert the error variant and its details.

## Applying the policy

Apply these definitions to new and modified error paths. Some existing names
predate this policy and may not fit their current conditions. Review the actual
guard and its callers rather than treating an existing name as authoritative.
Keep unrelated naming migrations separate from the change being reviewed.

For reachable guards, tests should execute the branch that constructs the error.
A test that fails at an earlier parsing or planning stage does not verify an
executor error. For `Unreachable` guards, review the conditions that prove
unreachability; if a test can reach the branch with accepted inputs, reconsider
the classification.
