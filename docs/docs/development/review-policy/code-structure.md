# Code Structure

## Minimize margin (guidance)

Margin is the gap between what a design allows or requires and what the code
actually needs. Keep that gap as small as practical when designing types,
functions, and modules.

### Represent valid states directly

Suppose a job has three states: pending, running, and finished. Two boolean
fields can represent them, but also allow a fourth combination:

```rust
struct Job {
    started: bool,
    finished: bool,
}
```

| `started` | `finished` | Meaning |
| --- | --- | --- |
| `false` | `false` | Pending |
| `true` | `false` | Running |
| `true` | `true` | Finished |
| `false` | `true` | Invalid: finished without starting |

Use an enum to represent the three valid states directly:

```rust
enum JobState {
    Pending,
    Running,
    Finished,
}

struct Job {
    state: JobState,
}
```

The invalid combination is no longer representable. Callers do not need to
preserve a relationship between two independent fields.

### Share common work without forcing operations together

Suppose tags must be trimmed and lowercased, while titles must only be trimmed.
Combining these operations introduces a flag and a branch:

```rust
fn normalize(text: &str, for_tag: bool) -> String {
    let text = text.trim();

    if for_tag {
        text.to_lowercase()
    } else {
        text.to_owned()
    }
}
```

In this example, callers always pass `true` for tags and `false` for titles.
The flag was added only to combine the two operations. Keep them separate:

```rust
fn normalize_tag(text: &str) -> String {
    text.trim().to_lowercase()
}

fn normalize_title(text: &str) -> String {
    text.trim().to_owned()
}
```

Repeating the small `trim()` call keeps each function's behavior explicit and
removes the mode flag and branch. Common work can still be shared without
combining the operations.

Apply the same reasoning to module boundaries. Sharing a helper should not
require moving unrelated caller-specific types or dependencies into the shared
module.

These are design guidelines. Some margin may remain when removing it would
make the overall design more complex.

## Visibility

Use the narrowest visibility needed by callers and the intended API. Apply
this to functions, types, fields, and re-exports. Sharing code does not
automatically require `pub(crate)` or `pub`.

Check both module visibility and re-exports to see who can access an item.
For example, a `pub` function in a private module can be exposed through a
`pub(super)` re-export.

## Declaration order (guidance)

Prefer an order that lets readers follow the code from top to bottom with
fewer jumps between declarations.

Use this as a guide for new modules. Leave unrelated code in place when
editing existing files.
