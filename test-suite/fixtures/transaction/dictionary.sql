CREATE TABLE Garlic (id INTEGER);

-- expect: ok

SHOW TABLES;

-- expect: payload ShowVariable.Tables
-- [
--   "Garlic"
-- ]

BEGIN;

-- expect: ok

SHOW TABLES;

-- expect: payload ShowVariable.Tables
-- [
--   "Garlic"
-- ]

CREATE TABLE Noodle (id INTEGER);

-- expect: ok

SHOW TABLES;

-- expect: payload ShowVariable.Tables
-- [
--   "Garlic",
--   "Noodle"
-- ]

ROLLBACK;

-- expect: ok

SHOW TABLES;

-- expect: payload ShowVariable.Tables
-- [
--   "Garlic"
-- ]

BEGIN;

-- expect: ok

CREATE TABLE Apple (id INTEGER);

-- expect: ok

CREATE TABLE Rice (id INTEGER);

-- expect: ok

SHOW TABLES;

-- expect: payload ShowVariable.Tables
-- [
--   "Apple",
--   "Garlic",
--   "Rice"
-- ]

COMMIT;

-- expect: ok

SHOW TABLES;

-- expect: payload ShowVariable.Tables
-- [
--   "Apple",
--   "Garlic",
--   "Rice"
-- ]
