CREATE TABLE User (
    id INTEGER,
    num INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO User (id, num, name)
VALUES
    (1, 2, 'Hello'),
    (2, 4, 'World'),
    (3, 9, 'Office'),
    (4, 1, 'Origin'),
    (5, 2, 'Builder');
-- @expect: ok

CREATE INDEX idx_id ON User (id);
-- @expect: payload CreateIndex

SELECT * FROM User u1
WHERE (
    SELECT u1.id = id FROM User
    WHERE id = 1
    LIMIT 1
);
-- @expect-index: idx_id = 1
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT * FROM User u1
WHERE EXISTS(
    SELECT * FROM User
    WHERE id = 1 AND u1.id = id
);
-- @expect-index: idx_id = 1
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT * FROM User u1
WHERE id IN (
    SELECT id FROM User WHERE id = 1
);
-- @expect-index: idx_id = 1
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
