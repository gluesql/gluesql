-- Input storage state: metadata=2, redb file format 2, (Key, Vec<Value>) rows.
-- Fixture origin: released GlueSQL v0.19.0 schema and v0.20.0 upgraded rows.
-- v2 route fixture: copy raw schemas and rows from ../v3/upgraded_v1_typed_default.redb.
-- No AST/tag changes; metadata is 2 and redb file format is 2.
-- Generator: ../generate.rs, compiled against the v0.20.0 release checkout.
-- Dependencies and release source identifiers: see ../README.md and source sidecars.
-- Command: cargo run --locked --offline --manifest-path <v0.20.0>/fixture-writer/Cargo.toml -- typed_default.redb v2 ../v3/upgraded_v1_typed_default.redb
-- SHA-256: 298e3d9a6b5446343764899b61ebb26f7a1fd68f85f2c0c2912738cfb2e67346

CREATE TABLE Ambiguous (
    id INT PRIMARY KEY,
    flag BOOLEAN DEFAULT TEXT '2' BETWEEN TEXT '10' AND TEXT '9'
);
INSERT INTO Ambiguous (id) VALUES (1);
-- Actual historical result: (1, true). The source schema is from released v0.19.0.
SELECT * FROM Ambiguous;
