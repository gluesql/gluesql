-- @name: basic usage - LIKE and ILIKE
VALUES
    ('abc' LIKE '%c'),
    ('abc' NOT LIKE '_c'),
    ('abc' LIKE '_b_'),
    ('HELLO' ILIKE '%el%'),
    ('HELLO' NOT ILIKE '_ELLE');
-- @expect:
-- | column1: Bool |
-- | ------------- |
-- | true          |
-- | true          |
-- | true          |
-- | true          |
-- | true          |

CREATE TABLE Pattern (
    id INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO Pattern VALUES
    (1, 'Abc'),
    (2,  NULL);
-- @expect: ok

-- @name: LIKE and NOT LIKE preserve NULL in a nullable column
SELECT
    id,
    name LIKE 'a%' AS matched,
    name NOT LIKE 'a%' AS negated
FROM Pattern;
-- @expect:
-- | id: I64 | matched     | negated    |
-- | ------- | ----------- | ---------- |
-- | 1       | Bool(false) | Bool(true) |
-- | 2       | NULL        | NULL       |

-- @name: ILIKE shares NULL handling while retaining case-insensitive matching
SELECT
    id,
    name ILIKE 'a%' AS matched,
    name NOT ILIKE 'a%' AS negated
FROM Pattern;
-- @expect:
-- | id: I64 | matched    | negated     |
-- | ------- | ---------- | ----------- |
-- | 1       | Bool(true) | Bool(false) |
-- | 2       | NULL       | NULL        |

-- @name: WHERE LIKE skips a nullable string without failing the query
SELECT id
FROM Pattern
WHERE name LIKE 'A%';
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |

CREATE TABLE Item (
    id INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO Item (id, name) VALUES
    (1,    'Amelia'),
    (2,      'Doll'),
    (3, 'Gascoigne'),
    (4,   'Gehrman'),
    (5,     'Maria');
-- @expect: ok

SELECT name FROM Item WHERE name LIKE '_a%'
-- @expect: count 2

SELECT name FROM Item WHERE name LIKE '%r%'
-- @expect: count 2

SELECT name FROM Item WHERE SUBSTR(name, 1) LIKE '%a'
-- @expect: count 2

SELECT name FROM Item WHERE 'name' LIKE SUBSTR('%a', 1)
-- @expect: count 0

SELECT name FROM Item WHERE SUBSTR(name, 1) LIKE SUBSTR('%a', 1)
-- @expect: count 2

SELECT name FROM Item WHERE SUBSTR(name, 1) LIKE SUBSTR('%a', 1)
-- @expect: count 2

SELECT name FROM Item WHERE LOWER(name) LIKE SUBSTR('%a', 1)
-- @expect: count 2

SELECT name FROM Item WHERE SUBSTR(name, 1) LIKE '%' || LOWER('A')
-- @expect: count 2

SELECT name FROM Item WHERE name LIKE '%%'
-- @expect: count 5

SELECT name FROM Item WHERE name LIKE 'g%'
-- @expect: count 0

SELECT name FROM Item WHERE name ILIKE '_A%'
-- @expect: count 2

SELECT name FROM Item WHERE name ILIKE 'g%'
-- @expect: count 2

SELECT name FROM Item WHERE name ILIKE '%%'
-- @expect: count 5

SELECT name FROM Item WHERE name NOT LIKE '%a%'
-- @expect: count 1

SELECT name FROM Item WHERE name NOT ILIKE '%A%'
-- @expect: count 1

SELECT name FROM Item WHERE 'ABC' LIKE '_B_'
-- @expect: count 5

SELECT name FROM Item WHERE 'abc' ILIKE '_B_'
-- @expect: count 5

SELECT name FROM Item WHERE 'ABC' ILIKE '_B_'
-- @expect: count 5

SELECT name FROM Item WHERE 'ABC' LIKE 10
-- @expect: error Evaluate.LikeOnNonStringLiteral
-- @json:
-- {
--   "base": "ABC",
--   "case_sensitive": true,
--   "pattern": "10"
-- }

SELECT name FROM Item WHERE True ILIKE '_B_'
-- @expect: error Value.LikeOnNonString
-- @json:
-- {
--   "base": {
--     "Bool": true
--   },
--   "case_sensitive": false,
--   "pattern": {
--     "Str": "_B_"
--   }
-- }

SELECT name FROM Item WHERE name = 'Amelia' AND name LIKE 10
-- @expect: error Value.LikeOnNonString
-- @json:
-- {
--   "base": {
--     "Str": "Amelia"
--   },
--   "case_sensitive": true,
--   "pattern": {
--     "I64": 10
--   }
-- }

SELECT name FROM Item WHERE name = 'Amelia' AND name ILIKE 10
-- @expect: error Value.LikeOnNonString
-- @json:
-- {
--   "base": {
--     "Str": "Amelia"
--   },
--   "case_sensitive": false,
--   "pattern": {
--     "I64": 10
--   }
-- }
