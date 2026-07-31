CREATE TABLE Item (
    id INTEGER,
    quantity INTEGER NULL,
    age INTEGER NULL,
    total INTEGER
);
-- @expect: ok

INSERT INTO Item (id, quantity, age, total) VALUES
    (1, NULL,   11, 1),
    (2,  0,   90, 2),
    (3,  9, NULL, 3),
    (4,  3,    3, 1),
    (5, 25, NULL, 1),
    (6, 15,   11, 2),
    (7, 20,   90, 1),
    (1, NULL, 11, 1);
-- @expect: ok

SELECT COUNT(*) FROM Item;
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 8             |

SELECT COUNT(age), COUNT(quantity) FROM Item;
-- @expect:
-- | COUNT(age): I64 | COUNT(quantity): I64 |
-- | --------------- | -------------------- |
-- | 6               | 6                    |

SELECT COUNT(NULL);
-- @expect:
-- | COUNT(NULL): I64 |
-- | ---------------- |
-- | 0                |

SELECT COUNT(DISTINCT id) FROM Item
-- @expect:
-- | COUNT(DISTINCT id): I64 |
-- | ----------------------- |
-- | 7                       |

SELECT COUNT(DISTINCT age) FROM Item
-- @expect:
-- | COUNT(DISTINCT age): I64 |
-- | ------------------------ |
-- | 3                        |

SELECT COUNT(age), COUNT(DISTINCT age) FROM Item
-- @expect:
-- | COUNT(age): I64 | COUNT(DISTINCT age): I64 |
-- | --------------- | ------------------------ |
-- | 6               | 3                        |

SELECT COUNT(DISTINCT *) FROM Item
-- @expect:
-- | COUNT(DISTINCT *): I64 |
-- | ---------------------- |
-- | 7                      |

CREATE TABLE EmptyItem (id INTEGER NULL);
-- @expect: ok

SELECT COUNT(*) FROM EmptyItem;
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 0             |

SELECT COUNT(id) FROM EmptyItem;
-- @expect:
-- | COUNT(id): I64 |
-- | -------------- |
-- | 0              |

-- @name: HAVING without GROUP BY uses global aggregation
SELECT COUNT(*) FROM Item HAVING COUNT(*) > 0;
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 8             |

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
SELECT 1 FROM Item HAVING COUNT(*) = 8;
-- @expect:
-- | 1: I64 |
-- | ------ |
-- | 1      |
