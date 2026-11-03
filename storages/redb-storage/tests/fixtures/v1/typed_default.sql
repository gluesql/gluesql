-- Input storage state: v1, no metadata table, (Key, DataRow) rows.
-- Fixture source SHA: 3b26cb7aa39f9cb7ec5da36a62b2f4ab5f7b5b72
-- Historical source profile: v019; generator: ../generate.rs (see ../README.md).
-- Dependencies: bincode 1.3.3; serde 1.0.205; chrono 0.4.38;
-- rust_decimal 1.35.0 (serde-str); bigdecimal 0.4.10 (serde, string-only);
-- redb 2.6.0; sqlparser 0.52.0; futures 0.3.30.
-- Historical GlueSQL SQL writer, compacted without upgrading.
-- Command: cargo run --locked --offline --manifest-path <checkout>/fixture-writer/Cargo.toml --features legacy-async -- typed_default.redb ambiguous
-- SHA-256: 5e9c8204d5076d13043af88fffbae9138030177ce5f05991a5a4f767b3922652

CREATE TABLE Ambiguous (
    id INT PRIMARY KEY,
    flag BOOLEAN DEFAULT TEXT '2' BETWEEN TEXT '10' AND TEXT '9'
);
INSERT INTO Ambiguous (id) VALUES (1);
-- Actual historical result: (1, true). The source schema is from released v0.19.0.
SELECT * FROM Ambiguous;
