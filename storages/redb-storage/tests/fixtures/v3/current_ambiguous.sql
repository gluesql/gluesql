-- Input storage state: metadata=3, redb file format 3, (Key, Vec<Value>) rows.
-- Fixture source SHA: fd704700902b805befc152537bffd596617b8c9e
-- Historical source profile: v020; generator: ../generate.rs (see ../README.md).
-- Dependencies: bincode 1.3.3; serde 1.0.205; chrono 0.4.38;
-- rust_decimal 1.35.0 (serde-str); bigdecimal 0.4.10 (serde, string-only);
-- redb 2.6.0; sqlparser 0.52.0; futures 0.3.34.
-- Created with historical GlueSQL storage/core APIs, then redb compact(); no raw tag patching.
-- Command: cargo run --locked --offline --manifest-path <checkout>/fixture-writer/Cargo.toml -- current_ambiguous.redb decimal
-- SHA-256: c80d512adcb5c12d2f53d669a18b27c864c51fe0b6b4ccdd6eff150089f7925a

CREATE TABLE Ambiguous (id INT PRIMARY KEY, flag BOOLEAN);
-- StoreMut schema.default = Between(Value::Decimal("2"), false,
--     Value::Decimal("10"), Value::Decimal("9")), not TypedString expressions.
INSERT INTO Ambiguous (id) VALUES (1);
-- Actual v0.20.0 writer result: (1, false), comparing decimals numerically.
SELECT * FROM Ambiguous;
-- Same full schema bytes as upgraded_v1_typed_default.redb.
