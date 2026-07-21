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

SELECT SUM(age) FROM Item
-- @expect:
-- | SUM(age) |
-- | -------- |
-- | NULL     |

SELECT SUM(id), SUM(quantity) FROM Item
-- @expect:
-- | SUM(id): I64 | SUM(quantity): I64 |
-- | ------------ | ------------------ |
-- | 15           | 47                 |

SELECT SUM(ifnull(age, 0)) from Item;
-- @expect:
-- | SUM(ifnull(age, 0)): I64 |
-- | ------------------------ |
-- | 104                      |

SELECT SUM(1 + 2) FROM Item;
-- @expect:
-- | SUM(1 + 2): I64 |
-- | --------------- |
-- | 15              |

SELECT SUM(id + 1) FROM Item;
-- @expect:
-- | SUM(id + 1): I64 |
-- | ---------------- |
-- | 20               |

SELECT SUM(id * quantity) FROM Item;
-- @expect:
-- | SUM(id * quantity): I64 |
-- | ----------------------- |
-- | 174                     |

SELECT SUM(CASE WHEN id > 3 THEN quantity ELSE 0 END) FROM Item;
-- @expect:
-- | SUM(CASE WHEN id > 3 THEN quantity ELSE 0 END): I64 |
-- | --------------------------------------------------- |
-- | 28                                                  |

SELECT SUM(DISTINCT id) FROM Item
-- @expect:
-- | SUM(DISTINCT id): I64 |
-- | --------------------- |
-- | 15                    |

SELECT SUM(DISTINCT age) FROM Item
-- @expect:
-- | SUM(DISTINCT age) |
-- | ----------------- |
-- | NULL              |

CREATE TABLE EmptyItem (id INTEGER NULL);
-- @expect: ok

SELECT SUM(id) FROM EmptyItem;
-- @expect:
-- | SUM(id) |
-- | ------- |
-- | NULL    |
