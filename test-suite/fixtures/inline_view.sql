CREATE TABLE InnerTable (
    id INTEGER,
    name TEXT
)

-- expect: payload Create

CREATE TABLE OuterTable (
    id INTEGER,
    name TEXT
)

-- expect: payload Create

INSERT INTO InnerTable VALUES (1, 'GLUE'), (2, 'SQL'), (3, 'SQL')

-- expect: payload Insert
-- 3

INSERT INTO OuterTable VALUES (1, 'WORKS!'), (2, 'EXTRA')

-- expect: payload Insert
-- 2

SELECT * FROM InnerTable

-- expect:
-- | id: I64 | name: Str |
-- | 1       | "GLUE"    |
-- | 2       | "SQL"     |
-- | 3       | "SQL"     |

SELECT *
    FROM (
        SELECT COUNT(*) AS cnt FROM InnerTable
    ) AS InlineView

-- expect:
-- | cnt: I64 |
-- | 3        |

SELECT *
    FROM (
        SELECT COUNT(*) AS cnt
        FROM InnerTable
        WHERE id > 1
    ) AS InlineView

-- expect:
-- | cnt: I64 |
-- | 2        |

SELECT *
    FROM (
        SELECT COUNT(*) FROM InnerTable
    ) AS InlineView

-- expect:
-- | COUNT(*): I64 |
-- | 3             |

SELECT *
    FROM (
        SELECT COUNT(*) AS cnt FROM InnerTable
    )

-- expect: error Translate.LackOfAlias

SELECT *
    FROM (
        SELECT *
        FROM (
            SELECT COUNT(*) AS cnt FROM InnerTable
        ) AS InlineView
    ) AS InlineView2

-- expect:
-- | cnt: I64 |
-- | 3        |

SELECT *
    FROM OuterTable
    JOIN (
        SELECT id, name FROM InnerTable
    ) AS InlineView ON OuterTable.id = InlineView.id

-- expect:
-- | id: I64 | name: Str | id: I64 | name: Str |
-- | 1       | "WORKS!"  | 1       | "GLUE"    |
-- | 2       | "EXTRA"   | 2       | "SQL"     |

SELECT *
    FROM OuterTable JOIN (
        SELECT name FROM InnerTable
    ) AS InlineView ON OuterTable.id = InlineView.id

-- expect: error Evaluate.CompoundIdentifierNotFound
-- {
--   "column_name": "id",
--   "table_alias": "InlineView"
-- }

SELECT *
    FROM OuterTable
    JOIN (
        SELECT id, name
        FROM InnerTable
        WHERE id = 1
    ) AS InlineView ON OuterTable.id = InlineView.id

-- expect:
-- | id: I64 | name: Str | id: I64 | name: Str |
-- | 1       | "WORKS!"  | 1       | "GLUE"    |

SELECT *
    FROM OuterTable JOIN (
        SELECT * FROM InnerTable
    ) AS InlineView ON OuterTable.id = InlineView.id

-- expect:
-- | id: I64 | name: Str | id: I64 | name: Str |
-- | 1       | "WORKS!"  | 1       | "GLUE"    |
-- | 2       | "EXTRA"   | 2       | "SQL"     |

SELECT *
    FROM OuterTable JOIN (
        SELECT InnerTable.* FROM InnerTable
    ) AS InlineView ON OuterTable.id = InlineView.id

-- expect:
-- | id: I64 | name: Str | id: I64 | name: Str |
-- | 1       | "WORKS!"  | 1       | "GLUE"    |
-- | 2       | "EXTRA"   | 2       | "SQL"     |

SELECT InlineView.*
    FROM OuterTable JOIN (
        SELECT InnerTable.*, 'once' AS literal FROM InnerTable
    ) AS InlineView ON OuterTable.id = InlineView.id

-- expect:
-- | id: I64 | name: Str | literal: Str |
-- | 1       | "GLUE"    | "once"       |
-- | 2       | "SQL"     | "once"       |

SELECT *
    FROM OuterTable
    JOIN (
        SELECT OuterTable.id, OuterTable.name
        FROM OuterTable
        JOIN (
            SELECT * FROM InnerTable
        ) AS InlineView ON OuterTable.id = InlineView.id
    ) AS InlineView2 ON OuterTable.id = InlineView2.id

-- expect:
-- | id: I64 | name: Str | id: I64 | name: Str |
-- | 1       | "WORKS!"  | 1       | "WORKS!"  |
-- | 2       | "EXTRA"   | 2       | "EXTRA"   |

SELECT *
    FROM (
        SELECT name, count(*) as cnt
        FROM InnerTable
        GROUP BY name
    ) AS InlineView

-- expect:
-- | name: Str | cnt: I64 |
-- | "GLUE"    | 1        |
-- | "SQL"     | 2        |

SELECT * FROM (
    SELECT *
    FROM InnerTable
    LIMIT 1
    ) AS InlineView

-- expect:
-- | id: I64 | name: Str |
-- | 1       | "GLUE"    |

SELECT * FROM (
    SELECT *
    FROM InnerTable
    OFFSET 2
    ) AS InlineView

-- expect:
-- | id: I64 | name: Str |
-- | 3       | "SQL"     |

SELECT * FROM (
    SELECT *
    FROM InnerTable
    ORDER BY id desc
    ) AS InlineView

-- expect:
-- | id: I64 | name: Str |
-- | 3       | "SQL"     |
-- | 2       | "SQL"     |
-- | 1       | "GLUE"    |

SELECT *
    FROM OuterTable, (
            SELECT id
            FROM InnerTable
            WHERE InnerTable.id = OuterTable.id
        ) AS InlineView

-- expect: error Translate.TooManyTables

SELECT DISTINCT id FROM OuterTable

-- expect:
-- | id: I64 |
-- | 1       |
-- | 2       |

SELECT *
    FROM (
        SELECT *
        FROM InnerTable
    ) AS InlineView
    Join OuterTable ON InlineView.id = OuterTable.id

-- expect:
-- | id: I64 | name: Str | id: I64 | name: Str |
-- | 1       | "GLUE"    | 1       | "WORKS!"  |
-- | 2       | "SQL"     | 2       | "EXTRA"   |
