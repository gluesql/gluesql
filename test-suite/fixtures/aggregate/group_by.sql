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

SELECT id, COUNT(*) FROM Item GROUP BY id
-- @expect:
-- | id: I64 | COUNT(*): I64 |
-- | 1       | 1             |
-- | 2       | 1             |
-- | 3       | 2             |
-- | 4       | 1             |
-- | 5       | 1             |

SELECT id FROM Item GROUP BY id
-- @expect:
-- | id: I64 |
-- | 1       |
-- | 2       |
-- | 3       |
-- | 4       |
-- | 5       |

SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city
-- @expect:
-- | SUM(quantity): I64 | COUNT(*): I64 | city: Str |
-- | 21                 | 2             | "Seoul"   |
-- | 0                  | 1             | "Dhaka"   |
-- | NULL               | 1             | "Beijing" |
-- | 30                 | 1             | "Daejeon" |
-- | 24                 | 1             | "Seattle" |

SELECT id, city FROM Item GROUP BY city
-- @expect:
-- | id: I64 | city: Str |
-- | 1       | "Seoul"   |
-- | 2       | "Dhaka"   |
-- | 3       | "Beijing" |
-- | 3       | "Daejeon" |
-- | 5       | "Seattle" |

SELECT ratio, COUNT(*) FROM Item GROUP BY ratio
-- @expect:
-- | ratio: F64 | COUNT(*): I64 |
-- | 0.2        | 2             |
-- | 6.11       | 2             |
-- | 1.1        | 2             |

SELECT ratio FROM Item GROUP BY id, city
-- @expect:
-- | ratio: F64 |
-- | 0.2        |
-- | 6.11       |
-- | 1.1        |
-- | 0.2        |
-- | 1.1        |
-- | 6.11       |

SELECT id, ratio FROM Item GROUP BY id, city HAVING ratio > 6
-- @expect:
-- | id: I64 | ratio: F64 |
-- | 2       | 6.11       |
-- | 5       | 6.11       |

SELECT SUM(quantity), COUNT(*), city FROM Item GROUP BY city HAVING COUNT(*) > 1
-- @expect:
-- | SUM(quantity): I64 | COUNT(*): I64 | city: Str |
-- | 21                 | 2             | "Seoul"   |

SELECT city FROM Item GROUP BY city HAVING COALESCE(COUNT(*), 0) > 1
-- @expect:
-- | city: Str |
-- | "Seoul"   |

CREATE TABLE Sub (id INTEGER);
-- @expect: ok

INSERT INTO Sub VALUES (101), (102), (103), (104), (105);
-- @expect: ok

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
-- | 101     |
-- | 102     |
-- | 103     |
