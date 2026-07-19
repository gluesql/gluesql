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
    (5, 25, NULL, 1),
    (6, 10,   11, 2),
    (7, 25,   90, 1);
-- @expect: ok

SELECT VARIANCE(age) FROM Item
-- @expect:
-- | VARIANCE(age) |
-- | ------------- |
-- | NULL          |

SELECT VARIANCE(id), VARIANCE(quantity) FROM Item
-- @expect:
-- | VARIANCE(id): F64 | VARIANCE(quantity): F64 |
-- | ----------------- | ----------------------- |
-- | 4.0               | 82.775510204082         |

SELECT VARIANCE(DISTINCT id) FROM Item
-- @expect:
-- | VARIANCE(DISTINCT id): F64 |
-- | -------------------------- |
-- | 4.0                        |

SELECT VARIANCE(DISTINCT age) FROM Item
-- @expect:
-- | VARIANCE(DISTINCT age) |
-- | ---------------------- |
-- | NULL                   |

SELECT VARIANCE(quantity), VARIANCE(DISTINCT quantity) FROM Item
-- @expect:
-- | VARIANCE(quantity): F64 | VARIANCE(DISTINCT quantity): F64 |
-- | ----------------------- | -------------------------------- |
-- | 82.775510204082         | 74.64                            |
