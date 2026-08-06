CREATE TABLE Item (
    id INTEGER,
    quantity INTEGER NULL,
    city TEXT,
    ratio FLOAT
);
-- @expect: ok

INSERT INTO Item (id, quantity, city, ratio) VALUES
    (1,   10,   'Seoul',  0.2),
    (2,    0,   'Dhaka', 6.11),
    (3, NULL, 'Beijing',  1.1),
    (3,   30, 'Daejeon',  0.2),
    (4,   11,   'Seoul',  1.1),
    (5,   24, 'Seattle', 6.11);
-- @expect: ok

CREATE TABLE EmptyItem (
    id INTEGER NULL,
    name TEXT
);
-- @expect: ok

CREATE TABLE Sub (id INTEGER);
-- @expect: ok

INSERT INTO Sub VALUES (101), (102), (103), (104), (105);
-- @expect: ok

SELECT id, ratio FROM Item GROUP BY id, city HAVING ratio > 6
-- @expect:
-- | id: I64 | ratio: F64 |
-- | ------- | ---------- |
-- | 2       | 6.11       |
-- | 5       | 6.11       |

SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city HAVING COUNT(*) > 1
-- @expect:
-- | SUM(quantity): I64 | COUNT(*): I64 | city: Str |
-- | ------------------ | ------------- | --------- |
-- | 21                 | 2             | "Seoul"   |

SELECT city FROM Item GROUP BY city HAVING COALESCE(COUNT(*), 0) > 1
-- @expect:
-- | city: Str |
-- | --------- |
-- | "Seoul"   |

-- @name: HAVING without GROUP BY uses global aggregation
SELECT COUNT(*) FROM Item HAVING COUNT(*) > 0;
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 6             |

-- @name: HAVING aggregate sees the empty global group
SELECT COUNT(*) FROM EmptyItem HAVING COUNT(*) = 0;
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 0             |

-- @name: HAVING without aggregate still uses one global group
SELECT 1 FROM Item HAVING TRUE;
-- @expect:
-- | 1: I64 |
-- | ------ |
-- | 1      |

SELECT 1 FROM Item HAVING FALSE;
-- @expect:
-- | 1: I64 |
-- | ------ |

SELECT 1 FROM EmptyItem HAVING TRUE;
-- @expect:
-- | 1: I64 |
-- | ------ |
-- | 1      |

-- @name: aggregate can appear only in HAVING
SELECT 1 FROM Item HAVING COUNT(*) = 6;
-- @expect:
-- | 1: I64 |
-- | ------ |
-- | 1      |

-- @name: HAVING - nested select context handling edge case
SELECT id
FROM Sub
WHERE (id - 100) IN (
    SELECT id
    FROM Item
    GROUP BY id
    HAVING id <= 3
)
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 101     |
-- | 102     |
-- | 103     |

-- @name: HAVING preserves correlated outer context
SELECT id
FROM Sub
WHERE EXISTS (
    SELECT 1
    FROM Item
    HAVING COUNT(*) = Sub.id - 99
)
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 105     |

-- @name: empty global aggregation preserves correlated outer context
SELECT id
FROM Sub
WHERE id = (
    SELECT Sub.id
    FROM Item
    WHERE FALSE
    HAVING TRUE
    ORDER BY Sub.id
)
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 101     |
-- | 102     |
-- | 103     |
-- | 104     |
-- | 105     |

-- @name: wildcard HAVING on an empty source preserves no rows
SELECT * FROM EmptyItem HAVING TRUE;
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |

SELECT EmptyItem.* FROM EmptyItem HAVING TRUE;
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
