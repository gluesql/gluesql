CREATE TABLE Test (
    id INTEGER,
    num INTEGER
)

-- expect: ok

CREATE TABLE OverflowTest (
    id INTEGER,
    num INTEGER
)

-- expect: ok

CREATE TABLE NullTest (
    id INTEGER,
    num INTEGER
)

-- expect: ok

INSERT INTO Test (id, num) VALUES (1, 1)

-- expect: ok

INSERT INTO Test (id, num) VALUES (1, 2)

-- expect: ok

INSERT INTO Test (id, num) VALUES (3, 4), (4, 8)

-- expect: ok

INSERT INTO OverflowTest (id, num) VALUES (1, 1)

-- expect: ok

INSERT INTO NullTest (id, num) VALUES (NULL, 1)

-- expect: ok

-- name: select all from table
SELECT (num << 1) as num FROM Test

-- expect:
-- | num: I64 |
-- | 2        |
-- | 4        |
-- | 8        |
-- | 16       |

-- name: test bit shift overflow
SELECT (num << 65) as overflowed FROM OverflowTest

-- expect: error Value.BinaryOperationOverflow
-- {
--   "lhs": {
--     "I64": 1
--   },
--   "operator": "BitwiseShiftLeft",
--   "rhs": {
--     "U32": 65
--   }
-- }

SELECT id, num FROM NullTest

-- expect:
-- | id   | num: I64 |
-- | NULL | 1        |
