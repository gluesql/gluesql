-- Input storage state: metadata=2, redb file format 2, (Key, Vec<Value>) rows.
-- Fixture: mixed_schema_schemaless.redb
-- Source commit: 2545eccb
-- Source: generated with redb 2.6.0 + bincode 1.3.3 using raw redb APIs
-- SHA-256: 60ce479d89d16d1f0d39a0ce75835ede12b9e99dad78bd5b1e4cb41b5fe0c17b
-- Source storage format: v2 (GlueSQL format v2, redb file format v2)

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
