CREATE TABLE Test (
    id INTEGER DEFAULT 1,
    num INTEGER NULL,
    name TEXT NOT NULL
);
-- expect: ok

-- name: basic insert - single item
INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hi boo');
-- expect: payload Insert
-- 1

-- name: insert multiple rows
INSERT INTO Test (id, num, name)
    VALUES
        (3, 9, 'Kitty!'),
        (2, 7, 'Monsters');
-- expect: payload Insert
-- 2

INSERT INTO Test VALUES(17, 30, 'Sullivan');
-- expect: payload Insert
-- 1

INSERT INTO Test (num, name) VALUES (28, 'Wazowski');
-- expect: payload Insert
-- 1

INSERT INTO Test (name) VALUES ('The end');
-- expect: payload Insert
-- 1

INSERT INTO Test (id, num) VALUES (1, 10);
-- expect: error Insert.LackOfRequiredColumn
-- "name"

SELECT * FROM Test;
-- expect:
-- | id: I64 | num: I64 | name: Str  |
-- | 1       | 2        | "Hi boo"   |
-- | 3       | 9        | "Kitty!"   |
-- | 2       | 7        | "Monsters" |
-- | 17      | 30       | "Sullivan" |
-- | 1       | 28       | "Wazowski" |
-- | 1       | NULL     | "The end"  |

CREATE TABLE Target AS SELECT * FROM Test WHERE 1 = 0;
-- expect: ok

-- name: insert into target from source
INSERT INTO Target SELECT * FROM Test;
-- expect: payload Insert
-- 6

-- name: target rows are equivalent to source rows
SELECT * FROM Target;
-- expect:
-- | id: I64 | num: I64 | name: Str  |
-- | 1       | 2        | "Hi boo"   |
-- | 3       | 9        | "Kitty!"   |
-- | 2       | 7        | "Monsters" |
-- | 17      | 30       | "Sullivan" |
-- | 1       | 28       | "Wazowski" |
-- | 1       | NULL     | "The end"  |

CREATE TABLE AggregateTarget (count INTEGER);
-- expect: ok

-- name: insert aggregate select result into target
INSERT INTO AggregateTarget SELECT COUNT(*) FROM Test;
-- expect: payload Insert
-- 1

-- name: aggregate insert result
SELECT * FROM AggregateTarget;
-- expect:
-- | count: I64 |
-- | 6          |
