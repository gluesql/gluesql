-- Fixture: mixed_schema_schemaless.redb
-- Source commit: 266c214d (pre-DataRow removal)
-- Source storage format: v1 (no __GLUESQL_META__ table)
-- Purpose: verify v1 -> v2 migration on both schema and schemaless tables.

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

-- ================================================================
-- 2) Representative queries after migrate_to_latest(...)
-- ================================================================
-- SELECT id, name, active FROM User ORDER BY id;
-- Expected rows:
-- (1, 'Alice', TRUE)
-- (2, 'Bob', FALSE)

-- INSERT INTO User VALUES (3, 'Carol', TRUE);
-- SELECT COUNT(*) AS cnt FROM User;
-- Expected: 3

-- SELECT kind, meta['ip'] AS ip FROM Event WHERE event_id = 1;
-- Expected row:
-- ('login', '10.0.0.1')

-- INSERT INTO Event VALUES
-- ('{"event_id":3,"kind":"logout","meta":{"ip":"10.0.0.3"}}');
-- SELECT kind FROM Event WHERE event_id = 3;
-- Expected row:
-- ('logout')
