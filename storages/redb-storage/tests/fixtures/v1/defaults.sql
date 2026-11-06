-- Input storage state: v1, no metadata table, (Key, DataRow) rows.
-- Fixture source SHA: 3b26cb7aa39f9cb7ec5da36a62b2f4ab5f7b5b72
-- Historical source profile: v019; generator: ../generate.rs (see ../README.md).
-- Dependencies: bincode 1.3.3; serde 1.0.205; chrono 0.4.38;
-- rust_decimal 1.35.0 (serde-str); bigdecimal 0.4.10 (serde, string-only);
-- redb 2.6.0; sqlparser 0.52.0; futures 0.3.30.
-- Created with historical GlueSQL storage/core APIs, then redb compact(); no raw tag patching.
-- Command: cargo run --locked --offline --manifest-path <checkout>/fixture-writer/Cargo.toml --features legacy-async -- defaults.redb defaults
-- SHA-256: 6a43ac19c1bac71ef996e510af5ae6201ccffc06efc3320db525562a234a8b2b

CREATE TABLE Defaults (
    id INT PRIMARY KEY,
    label TEXT DEFAULT 'legacy-default',
    score INT DEFAULT (2 + 3),
    ready BOOLEAN DEFAULT NOT FALSE,
    pattern BOOLEAN DEFAULT 'abc' LIKE 'a%'
);
INSERT INTO Defaults (id) VALUES (1);
-- Stored source row: (1, 'legacy-default', 5, true, true).
SELECT * FROM Defaults WHERE id = 1;
