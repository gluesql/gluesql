CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)
-- @expect: ok

CREATE TABLE TestA (
    id INTEGER,
    num INTEGER,
    name TEXT
)
-- @expect: ok

CREATE TABLE EmptyTest
-- @expect: ok

INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')
-- @expect: ok

INSERT INTO Test (id, num, name) VALUES (1, 9, 'World')
-- @expect: ok

INSERT INTO Test (id, num, name) VALUES (3, 4, 'Great'), (4, 7, 'Job')
-- @expect: ok

INSERT INTO TestA (id, num, name) SELECT id, num, name FROM Test
-- @expect: ok

CREATE TABLE TestB (id INTEGER);
-- @expect: ok

INSERT INTO TestB (id) SELECT id FROM Test
-- @expect: ok

-- @name: select all from table
SELECT * FROM TestB
-- @expect:
-- | id: I64 |
-- | 1       |
-- | 1       |
-- | 3       |
-- | 4       |

SELECT id, num, name FROM TestA
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 9        | "World"   |
-- | 3       | 4        | "Great"   |
-- | 4       | 7        | "Job"     |

SELECT * FROM EmptyTest
-- @expect: maps

SELECT * FROM (SELECT * FROM EmptyTest) AS Empty
-- @expect:
-- | _doc |

SELECT * FROM Test
-- @expect: count 4

UPDATE Test SET id = 2
-- @expect: ok

SELECT id FROM Test
-- @expect:
-- | id: I64 |
-- | 2       |
-- | 2       |
-- | 2       |
-- | 2       |

SELECT id, num FROM Test
-- @expect:
-- | id: I64 | num: I64 |
-- | 2       | 2        |
-- | 2       | 9        |
-- | 2       | 4        |
-- | 2       | 7        |

SELECT id FROM FOO.Test
-- @expect: error Translate.CompoundObjectNotSupported
-- @json: "FOO.Test"
