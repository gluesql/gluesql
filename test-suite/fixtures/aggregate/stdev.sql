CREATE TABLE Item (
    id INTEGER,
    quantity INTEGER,
    age INTEGER NULL,
    total INTEGER
);
-- @expect: ok

INSERT INTO Item (id, quantity, age, total) VALUES
    (1, 10,   11, 1),
    (2,  0,   90, 2),
    (3,  9, NULL, 3),
    (4,  3,    3, 1),
    (5, 25, NULL, 1);
-- @expect: ok

SELECT STDEV(age) FROM Item
-- @expect:
-- | STDEV(age) |
-- | ---------- |
-- | NULL       |

SELECT STDEV(total) FROM Item
-- @expect:
-- | STDEV(total): F64 |
-- | ----------------- |
-- | 0.8               |

SELECT STDEV(DISTINCT id) FROM Item
-- @expect:
-- | STDEV(DISTINCT id): F64 |
-- | ----------------------- |
-- | 1.414213562373          |

SELECT STDEV(DISTINCT age) FROM Item
-- @expect:
-- | STDEV(DISTINCT age) |
-- | ------------------- |
-- | NULL                |
