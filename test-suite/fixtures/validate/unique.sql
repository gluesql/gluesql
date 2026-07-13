CREATE TABLE TestA (
    id INTEGER UNIQUE,
    num INT
)
-- expect: ok

CREATE TABLE TestB (
    id INTEGER UNIQUE,
    num INT UNIQUE
)
-- expect: ok

CREATE TABLE TestC (
    id INTEGER NULL UNIQUE,
    num INT
)
-- expect: ok

INSERT INTO TestA VALUES (1, 1)
-- expect: ok

INSERT INTO TestA VALUES (2, 1), (3, 1)
-- expect: ok

INSERT INTO TestB VALUES (1, 1)
-- expect: ok

INSERT INTO TestB VALUES (2, 2), (3, 3)
-- expect: ok

INSERT INTO TestC VALUES (NULL, 1)
-- expect: ok

INSERT INTO TestC VALUES (2, 2), (NULL, 3)
-- expect: ok

UPDATE TestC SET id = 1 WHERE num = 1
-- expect: ok

UPDATE TestC SET id = NULL WHERE num = 1
-- expect: ok

INSERT INTO TestA VALUES (2, 2)
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 2
--   },
--   "id"
-- ]

INSERT INTO TestA VALUES (4, 4), (4, 5)
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 4
--   },
--   "id"
-- ]

UPDATE TestA SET id = 2 WHERE id = 1
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 2
--   },
--   "id"
-- ]

INSERT INTO TestB VALUES (1, 3)
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 1
--   },
--   "id"
-- ]

INSERT INTO TestB VALUES (4, 2)
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 2
--   },
--   "num"
-- ]

INSERT INTO TestB VALUES (5, 5), (6, 5)
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 5
--   },
--   "num"
-- ]

UPDATE TestB SET num = 2 WHERE id = 1
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 2
--   },
--   "num"
-- ]

INSERT INTO TestC VALUES (2, 4)
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 2
--   },
--   "id"
-- ]

INSERT INTO TestC VALUES (NULL, 5), (3, 5), (3, 6)
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 3
--   },
--   "id"
-- ]

UPDATE TestC SET id = 1
-- expect: error Validate.DuplicateEntryOnUniqueField
-- [
--   {
--     "I64": 1
--   },
--   "id"
-- ]
