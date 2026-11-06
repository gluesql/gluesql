-- Input storage state: metadata=3, redb file format 3, (Key, Vec<Value>) rows.
-- Fixture source SHA: fd704700902b805befc152537bffd596617b8c9e
-- Historical source profile: v020; generator: ../generate.rs (see ../README.md).
-- Dependencies: bincode 1.3.3; serde 1.0.205; chrono 0.4.38;
-- rust_decimal 1.35.0 (serde-str); bigdecimal 0.4.10 (serde, string-only);
-- redb 2.6.0; sqlparser 0.52.0; futures 0.3.34.
-- Created with historical GlueSQL storage/core APIs, then redb compact(); no raw tag patching.
-- Command: cargo run --locked --offline --manifest-path <checkout>/fixture-writer/Cargo.toml -- current_defaults.redb modern
-- SHA-256: a650098bb58813b3ba52a10e1499ba60fdd1d7ef89be4d993bdfcdade78b327f

CREATE TABLE Modern (
    id INT PRIMARY KEY,
    label TEXT DEFAULT 'modern-default',
    matches BOOLEAN DEFAULT 'abc' ~ 'a.*'
);
INSERT INTO Modern (id) VALUES (1);
SELECT * FROM Modern ORDER BY id;
