CREATE TABLE Item (
    id INTEGER,
    quantity INTEGER,
    age INTEGER NULL,
    total INTEGER
);

-- expect: ok

INSERT INTO Item (id, quantity, age, total) VALUES
    (1, 10,   11, 1),
    (2,  0,   90, 2),
    (3,  9, NULL, 3),
    (4,  3,    3, 1),
    (5, 25, NULL, 1);

-- expect: ok

SELECT AVG(age) FROM Item

-- expect:
-- | AVG(age) |
-- | NULL     |

SELECT AVG(id), AVG(quantity) FROM Item

-- expect:
-- | AVG(id): F64 | AVG(quantity): F64 |
-- | 3.0          | 9.4                |

SELECT AVG(DISTINCT id) FROM Item

-- expect:
-- | AVG(DISTINCT id): F64 |
-- | 3.0                   |

SELECT AVG(DISTINCT age) FROM Item

-- expect:
-- | AVG(DISTINCT age) |
-- | NULL              |
