CREATE TABLE TestA (
    id INTEGER UNIQUE,
    num INT
)
-- @expect: ok

CREATE TABLE TestB (
    id INTEGER UNIQUE,
    num INT UNIQUE
)
-- @expect: ok

CREATE TABLE TestC (
    id INTEGER NULL UNIQUE,
    num INT
)
-- @expect: ok

INSERT INTO TestA VALUES (1, 1)
-- @expect: ok

INSERT INTO TestA VALUES (2, 1), (3, 1)
-- @expect: ok

INSERT INTO TestB VALUES (1, 1)
-- @expect: ok

INSERT INTO TestB VALUES (2, 2), (3, 3)
-- @expect: ok

INSERT INTO TestC VALUES (NULL, 1)
-- @expect: ok

INSERT INTO TestC VALUES (2, 2), (NULL, 3)
-- @expect: ok

UPDATE TestC SET id = 1 WHERE num = 1
-- @expect: ok

UPDATE TestC SET id = NULL WHERE num = 1
-- @expect: ok

INSERT INTO TestA VALUES (2, 2)
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 2
--   },
--   "id"
-- ]

INSERT INTO TestA VALUES (4, 4), (4, 5)
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 4
--   },
--   "id"
-- ]

UPDATE TestA SET id = 2 WHERE id = 1
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 2
--   },
--   "id"
-- ]

INSERT INTO TestB VALUES (1, 3)
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 1
--   },
--   "id"
-- ]

INSERT INTO TestB VALUES (4, 2)
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 2
--   },
--   "num"
-- ]

INSERT INTO TestB VALUES (5, 5), (6, 5)
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 5
--   },
--   "num"
-- ]

UPDATE TestB SET num = 2 WHERE id = 1
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 2
--   },
--   "num"
-- ]

INSERT INTO TestC VALUES (2, 4)
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 2
--   },
--   "id"
-- ]

INSERT INTO TestC VALUES (NULL, 5), (3, 5), (3, 6)
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 3
--   },
--   "id"
-- ]

UPDATE TestC SET id = 1
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "I64": 1
--   },
--   "id"
-- ]

-- @name: UPDATE checks only the unique columns assigned in SET
UPDATE TestA SET num = 9 WHERE id = 2;
-- @expect: payload Update
-- @json: 1

SELECT id, num FROM TestA WHERE id = 2;
-- @expect:
-- | id: I64 | num: I64 |
-- | ------- | -------- |
-- | 2       | 9        |

CREATE TABLE WithPrimaryKey (id INTEGER PRIMARY KEY, email TEXT UNIQUE);
-- @expect: ok

INSERT INTO WithPrimaryKey VALUES (1, 'a');
-- @expect: payload Insert
-- @json: 1

-- @name: INSERT checks non-primary unique columns even when a primary key exists
INSERT INTO WithPrimaryKey VALUES (2, 'a');
-- @expect: error Unique.DuplicateEntryOnUniqueField
-- @json: [{"Str": "a"}, "email"]

SELECT id, email FROM WithPrimaryKey ORDER BY id;
-- @expect:
-- | id: I64 | email: Str |
-- | ------- | ---------- |
-- | 1       | "a"        |
