CREATE TABLE Items (id INTEGER NOT NULL, name TEXT, status TEXT DEFAULT 'ACTIVE' NOT NULL);

-- expect: ok

VALUES (1), (2), (3)

-- expect:
-- | column1: I64 |
-- | 1            |
-- | 2            |
-- | 3            |

VALUES (1, 'a'), (2, 'b')

-- expect:
-- | column1: I64 | column2: Str |
-- | 1            | "a"          |
-- | 2            | "b"          |

VALUES (1, 'a'), (2, 'b') ORDER BY column1 DESC

-- expect:
-- | column1: I64 | column2: Str |
-- | 2            | "b"          |
-- | 1            | "a"          |

VALUES (1), (2) limit 1

-- expect:
-- | column1: I64 |
-- | 1            |

VALUES (1), (2) offset 1

-- expect:
-- | column1: I64 |
-- | 2            |

VALUES (1, NULL), (2, NULL)

-- expect:
-- | column1: I64 | column2 |
-- | 1            | NULL    |
-- | 2            | NULL    |

VALUES (1), (2, 'b')

-- expect: error Select.NumberOfValuesDifferent

VALUES (1, 'a'), (2)

-- expect: error Select.NumberOfValuesDifferent

VALUES (1, 'a'), (2, 3)

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Text",
--   "literal": "3"
-- }

VALUES (1, 'a'), ('b', 'c')

-- expect: error Evaluate.TextParseFailed
-- {
--   "data_type": "Int",
--   "literal": "b"
-- }

VALUES (1, NULL), (2, 'a'), (3, 4)

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Text",
--   "literal": "4"
-- }

CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)

-- expect: payload Create

SELECT * FROM TableFromValues

-- expect:
-- | column1: I64 | column2: Str | column3: Bool | column4: I64 | column5 |
-- | 1            | "a"          | true          | NULL         | NULL    |
-- | 2            | "b"          | false         | 3            | NULL    |

SHOW COLUMNS FROM TableFromValues

-- expect: payload ShowColumns
-- [
--   [
--     "column1",
--     "Int"
--   ],
--   [
--     "column2",
--     "Text"
--   ],
--   [
--     "column3",
--     "Boolean"
--   ],
--   [
--     "column4",
--     "Int"
--   ],
--   [
--     "column5",
--     "Text"
--   ]
-- ]

SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived

-- expect:
-- | column1: I64 | column2: Str |
-- | 1            | "a"          |
-- | 2            | "b"          |

SELECT column1 AS id, column2 AS name FROM (VALUES (1, 'a'), (2, 'b')) AS Derived

-- expect:
-- | id: I64 | name: Str |
-- | 1       | "a"       |
-- | 2       | "b"       |

INSERT INTO Items (id) VALUES (1);

-- expect: payload Insert
-- 1

INSERT INTO Items (id2) VALUES (1);

-- expect: error Insert.WrongColumnName
-- "id2"

INSERT INTO Items (name) VALUES ('glue');

-- expect: error Insert.LackOfRequiredColumn
-- "id"

INSERT INTO Items (id) VALUES (3, 'sql')

-- expect: error Insert.ColumnAndValuesNotMatched

INSERT INTO Items VALUES (100, 'a', 'b', 1);

-- expect: error Insert.TooManyValues

INSERT INTO Nothing VALUES (1);

-- expect: error Insert.TableNotFound
-- "Nothing"
