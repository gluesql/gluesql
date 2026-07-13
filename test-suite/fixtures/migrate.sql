CREATE TABLE Test (
    id INT,
    num INT,
    name TEXT
);

-- expect: ok

INSERT INTO Test (id, num, name) VALUES
    (1,     2,     'Hello'),
    (-(-1), 9,     'World'),
    (+3,    2 * 2, 'Great');

-- expect: ok

INSERT INTO Test (id, num, name) VALUES (1.1, 1, 'good');

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int",
--   "literal": "1.1"
-- }

INSERT INTO Test (id, num, name) VALUES (1, 1, a.b);

-- expect: error Evaluate.CompoundIdentifierRequiresRowContext
-- {
--   "alias": "a",
--   "ident": "b"
-- }

INSERT INTO Test (id, num, name) VALUES (1, 1, name);

-- expect: error Evaluate.IdentifierRequiresRowContext
-- "name"

SELECT * FROM Test WHERE Here.User.id = 1

-- expect: error Translate.UnsupportedExpr
-- "Here.User.id"

SELECT * FROM Test NATURAL JOIN Test

-- expect: error Translate.UnsupportedJoinConstraint
-- "NATURAL"

SELECT 1 ^ 2 FROM Test;

-- expect: error Translate.UnsupportedBinaryOperator
-- "^"

SELECT * FROM Test UNION SELECT * FROM Test;

-- expect: error Translate.UnsupportedQuerySetExpr
-- "SELECT * FROM Test UNION SELECT * FROM Test"

SELECT * FROM Test WHERE noname = 1;

-- expect: error Evaluate.IdentifierNotFound
-- "noname"

SELECT * FROM Nothing;

-- expect: error Fetch.TableNotFound
-- "Nothing"

TRUNCATE TABLE ProjectUser;

-- expect: error Translate.UnsupportedStatement
-- "TRUNCATE TABLE ProjectUser"

SELECT DISTINCT ON (id) id, num, name FROM Test;

-- expect: error Translate.SelectDistinctOnNotSupported

SELECT id, num, name FROM Test

-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 9        | "World"   |
-- | 3       | 4        | "Great"   |

SELECT id, num, name FROM Test WHERE id = 1

-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 9        | "World"   |

UPDATE Test SET id = 2

-- expect: ok

SELECT id, num, name FROM Test

-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 2       | 2        | "Hello"   |
-- | 2       | 9        | "World"   |
-- | 2       | 4        | "Great"   |

SELECT id FROM Test

-- expect:
-- | id: I64 |
-- | 2       |
-- | 2       |
-- | 2       |

SELECT id, num FROM Test

-- expect:
-- | id: I64 | num: I64 |
-- | 2       | 2        |
-- | 2       | 9        |
-- | 2       | 4        |

SELECT id, num FROM Test LIMIT 1 OFFSET 1

-- expect:
-- | id: I64 | num: I64 |
-- | 2       | 9        |

SELECT id, num FROM Test LIMIT 1 OFFSET 1

-- expect:
-- | id: I64 | num: I64 |
-- | 2       | 9        |
