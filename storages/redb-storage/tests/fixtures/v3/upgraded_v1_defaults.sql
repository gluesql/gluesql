-- Input storage state: metadata=3, redb file format 3, (Key, Vec<Value>) rows.
-- Fixture source SHA: fd704700902b805befc152537bffd596617b8c9e
-- Historical source profile: v020; generator: ../generate.rs (see ../README.md).
-- Dependencies: bincode 1.3.3; serde 1.0.205; chrono 0.4.38;
-- rust_decimal 1.35.0 (serde-str); bigdecimal 0.4.10 (serde, string-only);
-- redb 2.6.0; sqlparser 0.52.0; futures 0.3.34.
-- Created with historical GlueSQL storage/core APIs, then redb compact(); no raw tag patching.
-- Command: cargo run --locked --offline --manifest-path <checkout>/fixture-writer/Cargo.toml -- upgraded_v1_defaults.redb upgrade
-- SHA-256: bcde5124d62e857743ebfd6657cfe0ec2de05c91584c1fda346fae8646277d3e
-- Before running: copy ../v1/defaults.redb to this fixture path.
-- Original writer: 3b26cb7aa39f9cb7ec5da36a62b2f4ab5f7b5b72.
-- Then actual v0.20.0 migrate_to_latest: rows/file upgraded, schema bytes retained.

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
