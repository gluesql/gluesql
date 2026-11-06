-- Input storage state: v1, no metadata table, (Key, DataRow) rows.
-- Fixture source SHA: 3b26cb7aa39f9cb7ec5da36a62b2f4ab5f7b5b72
-- Historical source profile: v019; generator: ../generate.rs (see ../README.md).
-- Dependencies: bincode 1.3.3; serde 1.0.205; chrono 0.4.38;
-- rust_decimal 1.35.0 (serde-str); bigdecimal 0.4.10 (serde, string-only);
-- redb 2.6.0; sqlparser 0.52.0; futures 0.3.30.
-- Created with historical GlueSQL storage/core APIs, then redb compact(); no raw tag patching.
-- Command: cargo run --locked --offline --manifest-path <checkout>/fixture-writer/Cargo.toml --features legacy-async -- index_plan.redb index-plan
-- SHA-256: 4f9d7b19409141232748a64af5794a5596407b1e8cc3a6fc3e660428b5daa162

CREATE TABLE Queries (
    id INT,
    value INT DEFAULT (
        SELECT SUM(A.id + 1) FROM Numbers AS A
        INNER JOIN Numbers AS B ON A.id = B.id
        WHERE A.id > 1 GROUP BY A.id
        HAVING SUM(A.id) > 1 ORDER BY A.id LIMIT 3 OFFSET 1
    )
);
-- Created with Schema::from_ddl + StoreMut::insert_schema, not SQL execution.
-- No rows. SQL DEFAULT evaluation does not execute this subquery.
-- StoreMut AST: from.index=PrimaryKey(Value::I64(2)), selection=None.
