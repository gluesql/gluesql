## v0.19.0

## ✨ Highlights

### Indexed placeholder parameter binding
Added support for indexed placeholders (`$1`, `$2`, ...) for parameter binding ([#1800](https://github.com/gluesql/gluesql/pull/1800)).

```rust
// INSERT with multiple parameters
glue.execute_with_params(
    "INSERT INTO users (id, name) VALUES ($1, $2)",
    gluesql::params![1_i64, "Alice"],
).await?;

// SELECT with parameter
let rows = glue
    .execute_with_params(
        "SELECT name FROM users WHERE id = $1",
        gluesql::params![1_i64],
    )
    .await?;
```

- New `execute_with_params` and `plan_with_params` APIs
- New `params!` macro for building bound parameters
- Supports various parameter types: numbers, strings, dates/times, UUIDs, IPs, intervals, bytes

### Storage-specific query planner customization
Storages can now provide their own query planners via the new `Planner` trait ([#1825](https://github.com/gluesql/gluesql/pull/1825)).

The `Planner` trait transforms `Statement -> Statement`. Core provides these building blocks:
- `fetch_schema_map` - fetch schema information
- `validate` - validate the statement
- `plan_primary_key` - primary key planning
- `plan_index` - non-clustered index planning
- `plan_join` - join execution strategy (nested loop join, hash join)

You can combine these with your own planning logic:

```rust
// Default planner implementation
async fn plan(&self, statement: Statement) -> Result<Statement> {
    let schema_map = fetch_schema_map(self, &statement).await?;
    validate(&schema_map, &statement)?;
    let statement = plan_primary_key(&schema_map, statement);
    let statement = plan_join(&schema_map, statement);
    Ok(statement)
}

// Sled storage: adds index planning
async fn plan(&self, statement: Statement) -> Result<Statement> {
    let schema_map = fetch_schema_map(self, &statement).await?;
    validate(&schema_map, &statement)?;
    let statement = plan_primary_key(&schema_map, statement);
    let statement = plan_index(&schema_map, statement)?;  // index planning added
    let statement = plan_join(&schema_map, statement);
    Ok(statement)
}

// Custom: mix core functions with your own logic
async fn plan(&self, statement: Statement) -> Result<Statement> {
    let schema_map = fetch_schema_map(self, &statement).await?;
    validate(&schema_map, &statement)?;
    let statement = plan_primary_key(&schema_map, statement);
    let statement = plan_join(&schema_map, statement);
    let statement = my_custom_optimization(statement)?;  // your own planning
    Ok(statement)
}
```

> Note: Planner is optional. You can bypass it entirely by building AST directly via AST Builder and executing without planning.

### Direct runtime value injection with Expr::Value
Added `Expr::Value(Value)` variant for injecting runtime values directly into AST expressions ([#1860](https://github.com/gluesql/gluesql/pull/1860)).

```rust
use gluesql::core::ast_builder::value;
use gluesql::core::data::Value;

// Using value() function
let expr = value(Value::I64(100));
let expr = value(Value::Bool(true));

// Using From<Value> impl
let expr: ExprNode = Value::Str("hello".to_owned()).into();
```

- New `value()` function in AST Builder for injecting any `Value` into expressions
- `From<Value>` impl for `ExprNode` enables direct conversion
- `bytea()` now creates `Value::Bytea` directly (no hex encoding/decoding overhead)

### JSON arrow operator support
Added support for the `->` operator to access MAP keys and LIST indices ([#1807](https://github.com/gluesql/gluesql/pull/1807)).

- Accepts string or integer selectors for MAP keys
- Accepts integer selectors for LIST indices
- Returns `NULL` for missing keys or out-of-bounds indices
- Errors when base value isn't MAP/LIST or selector type is invalid

---

## 🐛 Bug Fixes
- Fixed f32/f64 division by Decimal incorrectly performing multiplication instead of division ([#1835](https://github.com/gluesql/gluesql/pull/1835))
- Disallowed Float32 as primary key / unique constraint to prevent precision issues ([#1827](https://github.com/gluesql/gluesql/pull/1827))
- Reject unsupported SQL clauses (WITH, FETCH, FOR UPDATE, INTO, TOP, WINDOW, etc.) with proper error messages during translation ([#1789](https://github.com/gluesql/gluesql/pull/1789))

---

## 🛠️ Refactoring

### Internal type system simplification
- Eliminated `data::Literal` type by absorbing into `Evaluated` enum ([#1845](https://github.com/gluesql/gluesql/pull/1845))
  - Removed intermediate `Literal`, `LiteralError`, `ConvertError` types
  - `Evaluated` now directly handles `Number(Cow<BigDecimal>)` and `Text(Cow<str>)`
  - Modularized evaluation: `binary_op`, `unary_op`, `cmp`, `eq`, `concat`, `like`
- Renamed `AstLiteral` to `Literal` since `data::Literal` no longer exists ([#1859](https://github.com/gluesql/gluesql/pull/1859))

### Other refactoring
- Refactored index planner to use shared expression helpers under `plan::expr` with deterministic/nullability checks ([#1823](https://github.com/gluesql/gluesql/pull/1823))
- Replaced custom `ControlFlowMap` trait with Rust stdlib `ControlFlow::map_continue` ([#1844](https://github.com/gluesql/gluesql/pull/1844))
- Boxed `Expr` and `Aggregate` payloads in error variants to reduce top-level Error size ([#1803](https://github.com/gluesql/gluesql/pull/1803))
- Refactored Parquet storage: replaced `lazy_static` with `LazyLock`, typed column writers, `TryFrom` instead of lossy casts ([#1806](https://github.com/gluesql/gluesql/pull/1806))
- Relaxed lifetime constraint on `TableNameNode::delete` from `'static` to `'a` for ergonomic use ([#1862](https://github.com/gluesql/gluesql/pull/1862))

---

## 🛠️ Tooling & Maintenance

### Clippy pedantic
- Prepare to apply `clippy::pedantic` progressively ([#1805](https://github.com/gluesql/gluesql/pull/1805))
- Apply clippy::cast_lossless clippy rule ([#1809](https://github.com/gluesql/gluesql/pull/1809))
- Apply `clippy::bool_to_int_with_if` clippy rule ([#1815](https://github.com/gluesql/gluesql/pull/1815))
- Apply `clippy::assigning_clones` clippy rule ([#1814](https://github.com/gluesql/gluesql/pull/1814))
- Apply `clippy::unnecessary_semicolon` clippy rule ([#1818](https://github.com/gluesql/gluesql/pull/1818))
- Tighten Clippy lints without code changes ([#1817](https://github.com/gluesql/gluesql/pull/1817))
- Enforce clippy doc markdown ([#1829](https://github.com/gluesql/gluesql/pull/1829))
- Apply some clippy pedantic rules ([#1830](https://github.com/gluesql/gluesql/pull/1830))
- Resolves more clippy rules ([#1833](https://github.com/gluesql/gluesql/pull/1833))
- Apply some clippy pedantic rules ([#1836](https://github.com/gluesql/gluesql/pull/1836))
- Apply `clippy::needless_pass_by_value` clippy rule ([#1837](https://github.com/gluesql/gluesql/pull/1837))
- Apply more pedantic clippy rules ([#1838](https://github.com/gluesql/gluesql/pull/1838))
- Apply pedantic clippy rules ([#1841](https://github.com/gluesql/gluesql/pull/1841))

### CI & workflows
- Remove release drafter GitHub Action ([#1802](https://github.com/gluesql/gluesql/pull/1802))
- Include gluesql-macros in publish workflow ([#1804](https://github.com/gluesql/gluesql/pull/1804))
- Pass PR number via coverage artifact ([#1811](https://github.com/gluesql/gluesql/pull/1811))
- Upgrade actions version ([#1856](https://github.com/gluesql/gluesql/pull/1856))

### Tests
- Organize expression tests under expr namespace ([#1808](https://github.com/gluesql/gluesql/pull/1808))
- Add null cases to literal bitwise_and tests ([#1812](https://github.com/gluesql/gluesql/pull/1812))
- Add unit tests for utils indexmap and vector modules ([#1813](https://github.com/gluesql/gluesql/pull/1813))

### Documentation
- Document personal agent instructions ([#1792](https://github.com/gluesql/gluesql/pull/1792))

### Dependencies
- Bump bigdecimal to 0.4.10 ([#1867](https://github.com/gluesql/gluesql/pull/1867))

---

## 👥 New Contributors
* @torrancew made their first contribution in [#1862](https://github.com/gluesql/gluesql/pull/1862)

---

## 👥 Contributors
[@moreal](https://github.com/moreal), [@panarch](https://github.com/panarch), [@reddevilmidzy](https://github.com/reddevilmidzy), [@SteelCrab](https://github.com/SteelCrab), [@torrancew](https://github.com/torrancew), [@zmrdltl](https://github.com/zmrdltl)

Thanks to everyone who contributed!

---

**Full Changelog**: https://github.com/gluesql/gluesql/compare/v0.18.0...v0.19.0
