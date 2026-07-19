-- Fixture: mixed_schema_schemaless.redb
-- Source commit: 2545eccb
-- Source: generated with redb 2.6.0 + bincode 1.3.3 using raw redb APIs
-- SHA-256: 60ce479d89d16d1f0d39a0ce75835ede12b9e99dad78bd5b1e4cb41b5fe0c17b
-- Source storage format: v2 (GlueSQL format v2, redb file format v2)
-- Purpose: verify v2 -> v3 migration on both schema and schemaless tables.

-- ================================================================
-- 1) Seed data written directly via redb + bincode (no GlueSQL API)
-- ================================================================
-- Schema table: CREATE TABLE User (id INTEGER, name TEXT, active BOOLEAN);
-- Rows (Key::I64 keys, Vec<Value> payloads):
--   (1, [I64(1),  Str("Alice"), Bool(true)])
--   (2, [I64(2),  Str("Bob"),   Bool(false)])

-- Schemaless table: CREATE TABLE Event;
-- Rows:
--   (1, [Map({ event_id:1, kind:"login",    meta:{ip:"10.0.0.1"}, tags:["auth","web"] })])
--   (2, [Map({ event_id:2, kind:"purchase", amount:199, meta:{ip:"10.0.0.2"} })])

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
