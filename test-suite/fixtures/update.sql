CREATE TABLE TableA (
    id INTEGER,
    num INTEGER,
    num2 INTEGER,
    name TEXT
)
-- expect: ok

INSERT INTO TableA (id, num, num2, name)
VALUES
    (1, 2, 4, 'Hello'),
    (1, 9, 5, 'World'),
    (3, 4, 7, 'Great'),
    (4, 7, 10, 'Job');
-- expect: ok

CREATE TABLE TableB (
    id INTEGER,
    num INTEGER,
    rank INTEGER
)
-- expect: ok

INSERT INTO TableB (id, num, rank)
VALUES
    (1, 2, 1),
    (1, 9, 2),
    (3, 4, 3),
    (4, 7, 4);
-- expect: ok

UPDATE TableA SET id = 2
-- expect: payload Update
-- 4

SELECT id, num FROM TableA
-- expect:
-- | id: I64 | num: I64 |
-- | 2       | 2        |
-- | 2       | 9        |
-- | 2       | 4        |
-- | 2       | 7        |

UPDATE TableA SET id = 4 WHERE num = 9
-- expect: payload Update
-- 1

UPDATE TableA SET name = SUBSTR('John', 1) WHERE num = 9
-- expect: payload Update
-- 1

SELECT id, num FROM TableA
-- expect:
-- | id: I64 | num: I64 |
-- | 2       | 2        |
-- | 4       | 9        |
-- | 2       | 4        |
-- | 2       | 7        |

UPDATE TableA SET num2 = (SELECT num FROM TableA WHERE num = 9 LIMIT 1) WHERE num = 9
-- expect: payload Update
-- 1

SELECT id, num, num2 FROM TableA
-- expect:
-- | id: I64 | num: I64 | num2: I64 |
-- | 2       | 2        | 4         |
-- | 4       | 9        | 9         |
-- | 2       | 4        | 7         |
-- | 2       | 7        | 10        |

UPDATE TableA SET num2 = (SELECT rank FROM TableB WHERE num = TableA.num) WHERE num = 7
-- expect: payload Update
-- 1

SELECT id, num, num2 FROM TableA
-- expect:
-- | id: I64 | num: I64 | num2: I64 |
-- | 2       | 2        | 4         |
-- | 4       | 9        | 9         |
-- | 2       | 4        | 7         |
-- | 2       | 7        | 4         |

UPDATE TableA SET num2 = (SELECT rank FROM TableB WHERE num = TableA.num) WHERE num = (SELECT MIN(num) FROM TableA)
-- expect: payload Update
-- 1

SELECT id, num, num2 FROM TableA
-- expect:
-- | id: I64 | num: I64 | num2: I64 |
-- | 2       | 2        | 1         |
-- | 4       | 9        | 9         |
-- | 2       | 4        | 7         |
-- | 2       | 7        | 4         |

CREATE TABLE ErrTestTable (id INTEGER);
-- expect: ok

INSERT INTO ErrTestTable (id) VALUES (1),(9);
-- expect: ok

UPDATE TableA INNER JOIN ErrTestTable ON 1 = 1 SET id = 1
-- expect: error Translate.JoinOnUpdateNotSupported

UPDATE (SELECT * FROM ErrTestTable) SET id = 1
-- expect: error Translate.UnsupportedTableFactor
-- "(SELECT * FROM ErrTestTable)"

UPDATE ErrTestTable SET ErrTestTable.id = 1 WHERE id = 1
-- expect: error Translate.CompoundIdentOnUpdateNotSupported
-- "ErrTestTable.id = 1"

UPDATE Nothing SET a = 1;
-- expect: error Execute.TableNotFound
-- "Nothing"

UPDATE TableA SET Foo = 1;
-- expect: error Update.ColumnNotFound
-- "Foo"
