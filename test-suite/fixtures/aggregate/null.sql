CREATE TABLE Item (
    id INTEGER,
    val INTEGER NULL
);
-- @expect: ok

INSERT INTO Item (id, val) VALUES
    (1, NULL),
    (2,    5),
    (3, NULL),
    (4,    3);
-- @expect: ok

-- @name: SUM skips NULL in the first row
SELECT SUM(val) FROM Item
-- @expect:
-- | SUM(val): I64 |
-- | ------------- |
-- | 8             |

-- @name: AVG divides by the number of non-NULL rows
SELECT AVG(val) FROM Item
-- @expect:
-- | AVG(val): F64 |
-- | ------------- |
-- | 4.0           |

-- @name: MIN skips a leading NULL instead of keeping it
SELECT MIN(val) FROM Item
-- @expect:
-- | MIN(val): I64 |
-- | ------------- |
-- | 3             |

-- @name: MAX skips a leading NULL instead of keeping it
SELECT MAX(val) FROM Item
-- @expect:
-- | MAX(val): I64 |
-- | ------------- |
-- | 5             |

-- @name: VARIANCE skips NULL
SELECT VARIANCE(val) FROM Item
-- @expect:
-- | VARIANCE(val): F64 |
-- | ------------------ |
-- | 1.0                |

-- @name: STDEV skips NULL
SELECT STDEV(val) FROM Item
-- @expect:
-- | STDEV(val): F64 |
-- | --------------- |
-- | 1.0             |

-- @name: COUNT keeps counting NULL rows only for the wildcard form
SELECT COUNT(val), COUNT(*) FROM Item
-- @expect:
-- | COUNT(val): I64 | COUNT(*): I64 |
-- | --------------- | ------------- |
-- | 2               | 4             |

-- @name: DISTINCT aggregates skip NULL as well
SELECT SUM(DISTINCT val), MIN(DISTINCT val), MAX(DISTINCT val) FROM Item
-- @expect:
-- | SUM(DISTINCT val): I64 | MIN(DISTINCT val): I64 | MAX(DISTINCT val): I64 |
-- | ---------------------- | ---------------------- | ---------------------- |
-- | 8                      | 3                      | 5                      |

CREATE TABLE AllNull (val INTEGER NULL);
-- @expect: ok

INSERT INTO AllNull VALUES (NULL), (NULL);
-- @expect: ok

-- @name: an all-NULL column aggregates to NULL
SELECT SUM(val), MIN(val), MAX(val), AVG(val), VARIANCE(val), STDEV(val) FROM AllNull
-- @expect:
-- | SUM(val) | MIN(val) | MAX(val) | AVG(val) | VARIANCE(val) | STDEV(val) |
-- | -------- | -------- | -------- | -------- | ------------- | ---------- |
-- | NULL     | NULL     | NULL     | NULL     | NULL          | NULL       |

-- @name: an all-NULL column still counts rows
SELECT COUNT(val), COUNT(*) FROM AllNull
-- @expect:
-- | COUNT(val): I64 | COUNT(*): I64 |
-- | --------------- | ------------- |
-- | 0               | 2             |

CREATE TABLE Grouped (city TEXT, val INTEGER NULL);
-- @expect: ok

INSERT INTO Grouped VALUES
    ('Seoul', NULL),
    ('Seoul',   10),
    ('Busan', NULL),
    ('Seoul',    2),
    ('Busan', NULL);
-- @expect: ok

-- @name: NULL elimination applies per group
SELECT city, SUM(val), MIN(val), COUNT(val) FROM Grouped GROUP BY city
-- @expect:
-- | city: Str | SUM(val) | MIN(val) | COUNT(val): I64 |
-- | --------- | -------- | -------- | --------------- |
-- | "Seoul"   | I64(12)  | I64(2)   | 2               |
-- | "Busan"   | NULL     | NULL     | 0               |
