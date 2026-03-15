-- Fixture: mixed_schema_schemaless/
-- Source storage format: v1 (no schema format header)
-- Purpose: verify v1 -> v2 migration on both schema and schemaless tables.
-- Expected migrated filesystem snapshot:
-- tests/fixtures/v1_to_v2/mixed_schema_schemaless/expected/

-- ================================================================
-- 1) Seed SQL represented by the fixture files
-- ================================================================
CREATE TABLE User (id INTEGER, name TEXT, active BOOLEAN);
INSERT INTO User VALUES
    (1, 'Alice', TRUE),
    (2, 'Bob', FALSE);

CREATE TABLE Event;
INSERT INTO Event VALUES
    ('{"event_id":1,"kind":"login","meta":{"ip":"10.0.0.1"},"tags":["auth","web"]}'),
    ('{"event_id":2,"kind":"purchase","amount":199,"meta":{"ip":"10.0.0.2"}}');
