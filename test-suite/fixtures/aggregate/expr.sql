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

-- @name: BETWEEN with aggregates
SELECT SUM(quantity) BETWEEN MIN(quantity) AND MAX(quantity) AS test FROM Item;
-- @expect:
-- | test: Bool |
-- | ---------- |
-- | false      |

-- @name: CASE comparing aggregates
SELECT CASE SUM(quantity) WHEN MIN(quantity) THEN MAX(id) ELSE COUNT(id) END AS test FROM Item;
-- @expect:
-- | test: I64 |
-- | --------- |
-- | 5         |

-- @name: CASE WHEN with aggregate condition
SELECT CASE WHEN SUM(quantity) > 30 THEN MAX(id) ELSE MIN(id) END AS test FROM Item;
-- @expect:
-- | test: I64 |
-- | --------- |
-- | 5         |

-- @name: wrapped aggregate inside scalar function
SELECT COALESCE(COUNT(*), 0) AS test FROM Item;
-- @expect:
-- | test: I64 |
-- | --------- |
-- | 5         |

-- @name: aggregate inside is null predicate
SELECT SUM(age) IS NULL AS test FROM Item;
-- @expect:
-- | test: Bool |
-- | ---------- |
-- | true       |
