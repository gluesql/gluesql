CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT NULL,
    rate FLOAT NULL
)
-- expect: ok

INSERT INTO Test (id, num, name, rate)
    VALUES
        (1, 2, 'Hello',    3.0),
        (1, 9, NULL,       NULL),
        (3, 4, 'World',    1.0),
        (4, 7, 'Thursday', NULL);
-- expect: ok

SELECT id, num FROM Test
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 2        |
-- | 1       | 9        |
-- | 3       | 4        |
-- | 4       | 7        |

SELECT id, num, name FROM Test ORDER BY id + num ASC
-- expect:
-- | id: I64 | num: I64 | name: Str  |
-- | 1       | 2        | "Hello"    |
-- | 3       | 4        | "World"    |
-- | 1       | 9        | NULL       |
-- | 4       | 7        | "Thursday" |

SELECT id, num, name FROM Test ORDER BY num DESC
-- expect:
-- | id: I64 | num: I64 | name: Str  |
-- | 1       | 9        | NULL       |
-- | 4       | 7        | "Thursday" |
-- | 3       | 4        | "World"    |
-- | 1       | 2        | "Hello"    |

SELECT id, num, name FROM Test ORDER BY name
-- expect:
-- | id: I64 | num: I64 | name: Str  |
-- | 1       | 2        | "Hello"    |
-- | 4       | 7        | "Thursday" |
-- | 3       | 4        | "World"    |
-- | 1       | 9        | NULL       |

SELECT id, num, name FROM Test ORDER BY name DESC
-- expect:
-- | id: I64 | num: I64 | name: Str  |
-- | 1       | 9        | NULL       |
-- | 3       | 4        | "World"    |
-- | 4       | 7        | "Thursday" |
-- | 1       | 2        | "Hello"    |

SELECT id, num, name, rate FROM Test ORDER BY rate DESC, id DESC
-- expect:
-- | id: I64 | num: I64 | name: Str  | rate: F64 |
-- | 4       | 7        | "Thursday" | NULL      |
-- | 1       | 9        | NULL       | NULL      |
-- | 1       | 2        | "Hello"    | 3.0       |
-- | 3       | 4        | "World"    | 1.0       |

SELECT id, num FROM Test ORDER BY id ASC, num DESC
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 9        |
-- | 1       | 2        |
-- | 3       | 4        |
-- | 4       | 7        |

SELECT id, num FROM Test
    ORDER BY
        (SELECT id FROM Test t2 WHERE Test.id = t2.id LIMIT 1) ASC,
        num DESC
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 9        |
-- | 1       | 2        |
-- | 3       | 4        |
-- | 4       | 7        |

SELECT id, num FROM Test
    ORDER BY
        (SELECT t2.id FROM Test t2
            WHERE Test.id = t2.id
            ORDER BY (Test.id + t2.id) LIMIT 1
        ) ASC,
        num DESC;
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 9        |
-- | 1       | 2        |
-- | 3       | 4        |
-- | 4       | 7        |

SELECT * FROM Test ORDER BY id NULLS FIRST
-- expect: error Translate.OrderByNullsFirstOrLastNotSupported

-- name: ORDER BY aliases
SELECT id AS C1, num AS C2 FROM Test ORDER BY C1 ASC, C2 DESC
-- expect:
-- | C1: I64 | C2: I64 |
-- | 1       | 9       |
-- | 1       | 2       |
-- | 3       | 4       |
-- | 4       | 7       |

-- name: original column_names still work even if aliases were used at SELECT clause
SELECT id AS C1, num AS C2 FROM Test ORDER BY id ASC, num DESC
-- expect:
-- | C1: I64 | C2: I64 |
-- | 1       | 9       |
-- | 1       | 2       |
-- | 3       | 4       |
-- | 4       | 7       |

-- name: ORDER BY I64 and UnaryOperator::PLUS work as COLUMN_INDEX
SELECT id, num FROM Test ORDER BY 1 ASC, +2 DESC
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 9        |
-- | 1       | 2        |
-- | 3       | 4        |
-- | 4       | 7        |

-- name: ORDER BY UnaryOperator::MINUS works as a normal integer
SELECT id, num FROM Test ORDER BY -1
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 2        |
-- | 1       | 9        |
-- | 3       | 4        |
-- | 4       | 7        |

-- name: ORDER BY COLUMN_INDEX should be larger than 0
SELECT id, num FROM Test ORDER BY 0
-- expect: error Sort.ColumnIndexOutOfRange
-- 0

-- name: ORDER BY COLUMN_INDEX should be less than the number of columns
SELECT id, num FROM Test ORDER BY 3
-- expect: error Sort.ColumnIndexOutOfRange
-- 3
