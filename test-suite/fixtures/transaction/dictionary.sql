CREATE TABLE Garlic (id INTEGER);
-- @expect: ok

SHOW TABLES;
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Garlic"
-- ]

BEGIN;
-- @expect: ok

SHOW TABLES;
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Garlic"
-- ]

CREATE TABLE Noodle (id INTEGER);
-- @expect: ok

SHOW TABLES;
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Garlic",
--   "Noodle"
-- ]

ROLLBACK;
-- @expect: ok

SHOW TABLES;
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Garlic"
-- ]

BEGIN;
-- @expect: ok

CREATE TABLE Apple (id INTEGER);
-- @expect: ok

CREATE TABLE Rice (id INTEGER);
-- @expect: ok

SHOW TABLES;
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Apple",
--   "Garlic",
--   "Rice"
-- ]

COMMIT;
-- @expect: ok

SHOW TABLES;
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Apple",
--   "Garlic",
--   "Rice"
-- ]
