-- Input storage state: metadata=2, redb file format 2, (Key, Vec<Value>) rows.
-- Fixture origin: released GlueSQL v0.19.0 schema and v0.20.0 upgraded rows.
-- v2 route fixture: copy raw schemas and rows from ../v3/upgraded_v1_defaults.redb.
-- No AST/tag changes; metadata is 2 and redb file format is 2.
-- Generator: ../generate.rs, compiled against the v0.20.0 release checkout.
-- Dependencies and release source identifiers: see ../README.md and source sidecars.
-- Command: cargo run --locked --offline --manifest-path <v0.20.0>/fixture-writer/Cargo.toml -- defaults.redb v2 ../v3/upgraded_v1_defaults.redb
-- SHA-256: 9e31e4daa44329cf7e4f404dbea0157e86f80af49aea67880d0669d538218f0b

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
