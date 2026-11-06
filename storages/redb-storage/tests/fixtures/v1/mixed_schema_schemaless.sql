-- Input storage state: v1, no metadata table, (Key, DataRow) rows.
-- Fixture: mixed_schema_schemaless.redb
-- Source commit: 266c214d (pre-DataRow removal)
-- Source storage format: v1 (no __GLUESQL_META__ table)
-- SHA-256: 984f4d5e77da49914e1a8eb5e5f666c9b6253a671747878749da264a6e956415

-- ================================================================
-- 1) Seed SQL used to create the fixture
-- ================================================================
CREATE TABLE User (id INTEGER, name TEXT, active BOOLEAN);
INSERT INTO User VALUES
    (1, 'Alice', TRUE),
    (2, 'Bob', FALSE);

CREATE TABLE Event;
INSERT INTO Event VALUES
    ('{"event_id":1,"kind":"login","meta":{"ip":"10.0.0.1"},"tags":["auth","web"]}'),
    ('{"event_id":2,"kind":"purchase","amount":199,"meta":{"ip":"10.0.0.2"}}');
