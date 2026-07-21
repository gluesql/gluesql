CREATE TABLE Concat (
    id INTEGER,
    rate FLOAT,
    flag BOOLEAN,
    text TEXT,
    null_value TEXT NULL
);
-- @expect: ok

INSERT INTO Concat VALUES (1, 2.5, TRUE, 'Foo', NULL);
-- @expect: ok

SELECT
    text || text AS value_value,
    text || 'Bar' AS value_literal,
    'Bar' || text AS literal_value,
    'Foo' || 'Bar' AS literal_literal
FROM Concat;
-- @expect:
-- | value_value: Str | value_literal: Str | literal_value: Str | literal_literal: Str |
-- | ---------------- | ------------------ | ------------------ | -------------------- |
-- | "FooFoo"         | "FooBar"           | "BarFoo"           | "FooBar"             |

SELECT
    id || null_value AS id_n,
    rate || null_value AS rate_n,
    flag || null_value AS flag_n,
    text || null_value AS text_n,
    null_value || id AS n_id,
    null_value || text AS n_text
FROM
    Concat;
-- @expect:
-- | id_n | rate_n | flag_n | text_n | n_id | n_text |
-- | ---- | ------ | ------ | ------ | ---- | ------ |
-- | NULL | NULL   | NULL   | NULL   | NULL | NULL   |

SELECT
    id || CAST(rate * 10 AS INT) AS Case1,
    CAST(rate * 10 AS INT) || flag AS Case2,
    flag || text AS Case3,
    id || text AS Case4
FROM
    Concat;
-- @expect:
-- | Case1: Str | Case2: Str | Case3: Str | Case4: Str |
-- | ---------- | ---------- | ---------- | ---------- |
-- | "125"      | "25TRUE"   | "TRUEFoo"  | "1Foo"     |

SELECT
    1 || 2.5 AS int_float,
    2.5 || TRUE AS float_bool,
    FALSE || 'Foo' AS bool_text,
    1 || 'Bar' AS int_text
FROM
    Concat;
-- @expect:
-- | int_float: Str | float_bool: Str | bool_text: Str | int_text: Str |
-- | -------------- | --------------- | -------------- | ------------- |
-- | "12.5"         | "2.5TRUE"       | "FALSEFoo"     | "1Bar"        |

SELECT
    1 || id || CAST(rate * 10 AS INT) || 'Bar' AS Case1,
    id || flag || 3.5 || text AS Case2,
    flag || 'wow' || null_value AS Case3
FROM
    Concat;
-- @expect:
-- | Case1: Str | Case2: Str    | Case3 |
-- | ---------- | ------------- | ----- |
-- | "1125Bar"  | "1TRUE3.5Foo" | NULL  |

SELECT 'sand' || SUBSTR('swich', 2) AS test FROM Concat;
-- @expect:
-- | test: Str  |
-- | ---------- |
-- | "sandwich" |

SELECT SUBSTR('ssand', 2) || 'wich' AS test from Concat;
-- @expect:
-- | test: Str  |
-- | ---------- |
-- | "sandwich" |

SELECT LOWER('SAND') || SUBSTR('swich', 2) AS test FROM Concat;
-- @expect:
-- | test: Str  |
-- | ---------- |
-- | "sandwich" |

SELECT SUBSTR('ssand', 2) || LOWER('WICH') AS test FROM Concat;
-- @expect:
-- | test: Str  |
-- | ---------- |
-- | "sandwich" |
