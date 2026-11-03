-- Input storage state: metadata=3, redb file format 3, (Key, Vec<Value>) rows.
-- Fixture origin: GlueSQL v0.19.0 -> released v0.20.0 migrate_to_latest.
-- Released migration source: fd704700902b805befc152537bffd596617b8c9e.
-- Start with ../v1/typed_default.redb, then run the actual v0.20.0 migration.
-- Schema bytes remain v1; rows use (Key, Vec<Value>) and the file format is 3.
-- Dependencies and original source: see ../README.md and ../v1/typed_default.sql.
-- Command: cargo run --locked --offline --manifest-path <v0.20.0>/fixture-writer/Cargo.toml -- upgraded_v1_typed_default.redb upgrade
-- SHA-256: 5d935da70ca6f281745094be13806d445c83cb26380f286a926e40078ab5a956

CREATE TABLE Ambiguous (
    id INT PRIMARY KEY,
    flag BOOLEAN DEFAULT TEXT '2' BETWEEN TEXT '10' AND TEXT '9'
);
INSERT INTO Ambiguous (id) VALUES (1);
-- v0.19.0 inserted (1, true). v0.20.0 preserved the schema bytes during upgrade.
-- The same schema bytes also decode as a different current core schema.
SELECT * FROM Ambiguous;
