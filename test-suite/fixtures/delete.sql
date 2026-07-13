CREATE TABLE Foo (
    id INTEGER PRIMARY KEY,
    score INTEGER,
    flag BOOLEAN
);

-- expect: ok

INSERT INTO Foo VALUES
    (1, 100, TRUE),
    (2, 300, FALSE),
    (3, 700, TRUE);

-- expect: ok

SELECT * FROM Foo

-- expect:
-- | id: I64 | score: I64 | flag: Bool |
-- | 1       | 100        | true       |
-- | 2       | 300        | false      |
-- | 3       | 700        | true       |

-- name: delete using WHERE
DELETE FROM Foo WHERE flag = FALSE

-- expect: payload Delete
-- 1

SELECT * FROM Foo

-- expect:
-- | id: I64 | score: I64 | flag: Bool |
-- | 1       | 100        | true       |
-- | 3       | 700        | true       |

-- name: delete all
DELETE FROM Foo;

-- expect: payload Delete
-- 2

SELECT * FROM Foo

-- expect:
-- | id | score | flag |
