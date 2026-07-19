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

SELECT MIN(age) FROM Item
-- @expect:
-- | MIN(age): I64 |
-- | 3             |

SELECT MIN(id), MIN(quantity) FROM Item
-- @expect:
-- | MIN(id): I64 | MIN(quantity): I64 |
-- | 1            | 0                  |

SELECT MIN(id + quantity) FROM Item;
-- @expect:
-- | MIN(id + quantity): I64 |
-- | 2                       |

SELECT SUM(quantity) * 2 + MIN(quantity) - 3 / 1 FROM Item;
-- @expect:
-- | SUM(quantity) * 2 + MIN(quantity) - 3 / 1: I64 |
-- | 91                                             |

SELECT MIN(CASE WHEN quantity > 5 THEN id END) FROM Item;
-- @expect:
-- | MIN(CASE WHEN quantity > 5 THEN id END): I64 |
-- | 1                                            |

SELECT MIN(DISTINCT id) FROM Item
-- @expect:
-- | MIN(DISTINCT id): I64 |
-- | 1                     |

SELECT MIN(DISTINCT age) FROM Item
-- @expect:
-- | MIN(DISTINCT age): I64 |
-- | 3                      |
