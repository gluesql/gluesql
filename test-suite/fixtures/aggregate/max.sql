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

SELECT MAX(age) FROM Item
-- expect:
-- | MAX(age): I64 |
-- | 90            |

SELECT MAX(id), MAX(quantity) FROM Item
-- expect:
-- | MAX(id): I64 | MAX(quantity): I64 |
-- | 5            | 25                 |

SELECT MAX(id - quantity) FROM Item;
-- expect:
-- | MAX(id - quantity): I64 |
-- | 2                       |

SELECT SUM(quantity) * 2 + MAX(quantity) - 3 / 1 FROM Item
-- expect:
-- | SUM(quantity) * 2 + MAX(quantity) - 3 / 1: I64 |
-- | 116                                            |

SELECT MAX(DISTINCT id) FROM Item
-- expect:
-- | MAX(DISTINCT id): I64 |
-- | 5                     |

SELECT MAX(DISTINCT age) FROM Item
-- expect:
-- | MAX(DISTINCT age): I64 |
-- | 90                     |
