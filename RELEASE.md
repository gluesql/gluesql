# GlueSQL v0.18.0

## 🌊 Breaking Changes

- **Storage traits are now `Send + Sync`** – custom storages must switch from `Rc`/`RefCell` to thread-safe primitives to satisfy the new bounds introduced in [#1688](https://github.com/gluesql/gluesql/pull/1688). This lets GlueSQL operate cleanly in multi-threaded runtimes.
- **`Value::evaluate_eq` returns a tri-state result** – equality checks now produce a new `Tribool` type so NULL comparisons behave per SQL rules ([#1705](https://github.com/gluesql/gluesql/pull/1705)). If you called this helper directly, update call sites to handle `True`/`False`/`Null`.
- **`Statement::to_sql()` removed** – GlueSQL's AST is no longer serialised back to SQL strings. Consumers that relied on the method should build SQL using `sqlparser-rs` instead ([#1746](https://github.com/gluesql/gluesql/pull/1746)).
- **Minimum supported Rust version: 1.89** – the workspace toolchain moved to Rust 1.89 ([#1747](https://github.com/gluesql/gluesql/pull/1747), [#1762](https://github.com/gluesql/gluesql/pull/1762)). Update local toolchains and CI images before upgrading.

---

## ✨ Highlights

### Derive structs from query results (no manual row mapping!)
- The new `FromGlueRow` derive macro and `rows_as`/`one_as` helpers turn SELECT payloads into your own structs with zero boilerplate ([#1779](https://github.com/gluesql/gluesql/pull/1779)). The derive ships directly from the crate root, so any application can opt into typed results with a single attribute:

```rust
#[derive(gluesql::FromGlueRow)]
struct User {
    id: i64,
    name: String,
}

let users = glue
    .execute("SELECT id, name FROM users;")
    .await?
    .rows_as::<User>()?;

let first = glue
    .execute("SELECT id, name FROM users WHERE id = 1;")
    .await?
    .one_as::<User>()?;
```

- Result conversion works on individual `Payload`s or the entire `execute` result, and documentation/examples now showcase the pattern for immediate adoption.
- The derive now maps `UUID` values into `String` fields without extra helpers and qualifies its generated `Result` usage so local aliases continue to compile ([#1796](https://github.com/gluesql/gluesql/pull/1796), [#1797](https://github.com/gluesql/gluesql/pull/1797)); the README and hello world example were refreshed to match ([#1790](https://github.com/gluesql/gluesql/pull/1790)).

### Full `SELECT DISTINCT` support
- GlueSQL now recognises `DISTINCT` in projections and across aggregate functions, including `COUNT(DISTINCT *)`, `SUM(DISTINCT col)` and friends ([#1710](https://github.com/gluesql/gluesql/pull/1710)). AST Builder gained `.distinct()` helpers and the executor deduplicates rows during planning.

### New SQL functions
- Date & time: `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP` (also callable with parentheses) ([#1677](https://github.com/gluesql/gluesql/pull/1677)).
- Conditional: `NULLIF` ([#1683](https://github.com/gluesql/gluesql/pull/1683)).
- Numeric/string: `HEX` ([#1694](https://github.com/gluesql/gluesql/pull/1694)) and `TRUNC` ([#1787](https://github.com/gluesql/gluesql/pull/1787)).

### Improved NULL semantics & grouping
- Expression evaluation now short-circuits to NULL when any operand is NULL ([#1685](https://github.com/gluesql/gluesql/pull/1685)), and equality checks propagate tri-state truth ([#1705](https://github.com/gluesql/gluesql/pull/1705)).
- Aggregate keys use full `Value` instances instead of `Key`, enabling `GROUP BY` on `MAP` and `LIST` columns ([#1768](https://github.com/gluesql/gluesql/pull/1768)).
- `Value`, `Point` and related structures implement consistent `PartialEq`, `Eq`, and `Hash`, improving plan caching and set semantics ([#1724](https://github.com/gluesql/gluesql/pull/1724), [#1729](https://github.com/gluesql/gluesql/pull/1729)).

### Storage updates
- Redis storage depends on `redis` 0.32 and received mutex-based interiors for thread safety ([#1776](https://github.com/gluesql/gluesql/pull/1776), [#1688](https://github.com/gluesql/gluesql/pull/1688)).
- Redb storage shed unnecessary `async` layers for lower overhead ([#1782](https://github.com/gluesql/gluesql/pull/1782)).
- IndexedDB storage now guards against `Arc::try_unwrap` panics when multiple connections are open ([#1718](https://github.com/gluesql/gluesql/pull/1718)).
- Mongo storage surfaces conflict errors and handles schemaless array/double/null values more reliably ([#1654](https://github.com/gluesql/gluesql/pull/1654), [#1655](https://github.com/gluesql/gluesql/pull/1655)).

---

## 🐛 Bug Fixes
- Added coverage for Redis alter-table errors and MockStorage schema fetch failures ([#1715](https://github.com/gluesql/gluesql/pull/1715), [#1751](https://github.com/gluesql/gluesql/pull/1751)).
- Fixed IndexedDB multi-connection crash mentioned above ([#1718](https://github.com/gluesql/gluesql/pull/1718)).
- Patched MongoDB schemaless decoding regression ([#1654](https://github.com/gluesql/gluesql/pull/1654)).

---

## 📚 Documentation & Examples
- Added a Python package README and refreshed the web storage README with HTTP server guidance ([#1652](https://github.com/gluesql/gluesql/pull/1652), [#1670](https://github.com/gluesql/gluesql/pull/1670)).
- Updated contributor guidelines (AGENTS.md) to emphasise running relevant tests and grouped imports ([#1752](https://github.com/gluesql/gluesql/pull/1752), [#1788](https://github.com/gluesql/gluesql/pull/1788)).

---

## 🛠️ Tooling & Maintenance
- Coverage workflows moved to GitHub-hosted runners, publish artifacts to gluesql.org, and share condensed PR summaries ([#1757](https://github.com/gluesql/gluesql/pull/1757), [#1759](https://github.com/gluesql/gluesql/pull/1759), [#1761](https://github.com/gluesql/gluesql/pull/1761), [#1763](https://github.com/gluesql/gluesql/pull/1763), [#1769](https://github.com/gluesql/gluesql/pull/1769), [#1771](https://github.com/gluesql/gluesql/pull/1771), [#1773](https://github.com/gluesql/gluesql/pull/1773), [#1775](https://github.com/gluesql/gluesql/pull/1775), [#1781](https://github.com/gluesql/gluesql/pull/1781), [#1784](https://github.com/gluesql/gluesql/pull/1784)).
- Added Codecov configuration and uploads for more consistent diff coverage checks ([#1707](https://github.com/gluesql/gluesql/pull/1707), [#1730](https://github.com/gluesql/gluesql/pull/1730)).
- Miscellaneous CI and dependency refreshes: Docusaurus 3.8 ([#1681](https://github.com/gluesql/gluesql/pull/1681)), docs asset tweaks ([#1673](https://github.com/gluesql/gluesql/pull/1673)), and numerous workflow guardrails ([#1706](https://github.com/gluesql/gluesql/pull/1706), [#1697](https://github.com/gluesql/gluesql/pull/1697)).

---

## 👥 Contributors
[@chaerrypick01](https://github.com/chaerrypick01), [@dding-g](https://github.com/dding-g), [@dependabot](https://github.com/dependabot), [@google-labs-jules](https://github.com/google-labs-jules), [@junghoon-vans](https://github.com/junghoon-vans), [@miinhho](https://github.com/miinhho), [@moreal](https://github.com/moreal), [@panarch](https://github.com/panarch), [@reddevilmidzy](https://github.com/reddevilmidzy), [@SteelCrab](https://github.com/SteelCrab), [@zmrdltl](https://github.com/zmrdltl)

Thanks to everyone who contributed!

---

**Full Changelog**: [v0.17.0...v0.18.0](https://github.com/gluesql/gluesql/compare/v0.17.0...v0.18.0)
