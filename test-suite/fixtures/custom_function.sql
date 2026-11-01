CREATE FUNCTION add_none()
-- @expect: error Translate.UnsupportedEmptyFunctionBody

CREATE FUNCTION add_none() RETURN null
-- @expect: payload Create

CREATE FUNCTION add_zero(n INT) RETURN n
-- @expect: payload Create

CREATE FUNCTION add_zero(n INT) RETURN n
-- @expect: error Alter.FunctionAlreadyExists
-- @json: "add_zero"

CREATE FUNCTION add_one (n INT, x INT DEFAULT 1) RETURN n + x
-- @expect: payload Create

CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT) RETURN n + x + y
-- @expect: error Alter.NonDefaultArgumentFollowsDefaultArgument

CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT DEFAULT 1) RETURN n + x + y
-- @expect: payload Create

SELECT add_none() AS r
-- @expect:
-- | r    |
-- | ---- |
-- | NULL |

SELECT add_one(1) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 2      |

SELECT add_one(1, 8) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 9      |

SELECT add_one(1, 2, 4)
-- @expect: error Evaluate.FunctionArgsLengthNotWithinRange
-- @json:
-- {
--   "expected_maximum": 2,
--   "expected_minimum": 1,
--   "found": 3,
--   "name": "add_one"
-- }

SELECT add_one()
-- @expect: error Evaluate.FunctionArgsLengthNotWithinRange
-- @json:
-- {
--   "expected_maximum": 2,
--   "expected_minimum": 1,
--   "found": 0,
--   "name": "add_one"
-- }

SELECT add_two(1, null, 2) as r
-- @expect:
-- | r    |
-- | ---- |
-- | NULL |

SELECT add_two(1) as r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 3      |

DROP FUNCTION add_none
-- @expect: payload DropFunction

SHOW FUNCTIONS
-- @expect: payload ShowVariable.Functions
-- @json:
-- [
--   "add_one(n: INT, x: INT)",
--   "add_two(n: INT, x: INT, y: INT)",
--   "add_zero(n: INT)"
-- ]

DROP FUNCTION add_none
-- @expect: error Alter.FunctionNotFound
-- @json: "add_none"

DROP FUNCTION IF EXISTS add_zero, add_one, add_two
-- @expect: payload DropFunction

CREATE FUNCTION test(INT) RETURN 1
-- @expect: error Translate.UnNamedFunctionArgNotSupported

CREATE FUNCTION test(a INT DEFAULT test()) RETURN 1
-- @expect: error Evaluate.UnplannedCustomFunction
-- @json: "TEST"

CREATE FUNCTION test(a INT, a BOOLEAN) RETURN 1
-- @expect: error Alter.DuplicateArgName
-- @json: "a"

CREATE FUNCTION inc(x INT) RETURN x + 1
-- @expect: payload Create

-- @name: function body - scalar subquery
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT inc(x))
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 5      |

-- @name: function body - series size and filter
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT COUNT(*) FROM SERIES(x) WHERE N <= x)
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 4      |

-- @name: function body - custom call inside body aggregate
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT SUM(inc(N)) FROM SERIES(x))
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 14     |

-- @name: function body - values expression
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT column1 FROM (VALUES (inc(x))) v)
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 5      |

-- @name: function body - derived values and aggregate
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT SUM(v.column1) FROM (VALUES (x), (inc(x))) v)
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 9      |

-- @name: function body - series and offset
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT N FROM SERIES(x + 2) ORDER BY N DESC LIMIT 1 OFFSET x)
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 2      |

-- @name: function body - limit and offset
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT N FROM SERIES(3) LIMIT x - 3 OFFSET x - 3)
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 2      |

-- @name: function body - local column shadows argument
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT x FROM (VALUES (9)) v(x))
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 9      |

-- @name: function body - order by with argument
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT column1 FROM (VALUES (x), (x + 1)) v ORDER BY column1 + x DESC LIMIT 1)
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 5      |

-- @name: function body - nested query scopes and a nested call
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT (SELECT inc(v.n + x) FROM SERIES(v.n) LIMIT 1) FROM (VALUES (x + 1)) v(n))
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 10     |

-- @name: function body - joined source uses the current row and arguments
CREATE OR REPLACE FUNCTION local_query(x INT) RETURN (SELECT SUM(b.N) FROM SERIES(x) a JOIN SERIES(inc(a.N)) b ON a.N = b.N)
-- @expect: payload Create

SELECT local_query(4) AS r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 10     |

-- @name: ordinary VALUES executes prepared functions
VALUES (inc(1))
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 2            |

-- @name: ordinary SERIES executes prepared functions
SELECT * FROM SERIES(inc(1))
-- @expect:
-- | N: I64 |
-- | ------ |
-- | 1      |
-- | 2      |

-- @name: VALUES order and bounds execute prepared functions
VALUES (inc(1)), (inc(2)), (inc(3)) ORDER BY inc(column1) DESC LIMIT inc(0) OFFSET inc(0)
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 3            |

-- @name: ordinary subqueries use the same outer scope
SELECT (SELECT inc(v.n) FROM SERIES(v.n) LIMIT 1) AS r FROM (VALUES (2), (3)) v(n) ORDER BY r
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 3      |
-- | 4      |

CREATE TABLE Defaults (x INT DEFAULT inc(1))
-- @expect: error Evaluate.UnplannedCustomFunction
-- @json: "INC"

CREATE FUNCTION isolated() RETURN x
-- @expect: payload Create

-- @name: function body does not inherit the caller row
SELECT isolated() FROM (VALUES (9)) v(x)
-- @expect: error Evaluate.IdentifierNotFound
-- @json: "x"

-- @name: prepared calls in DML and CTAS
CREATE TABLE Item (x INT)
-- @expect: payload Create

INSERT INTO Item VALUES (inc(0)), (inc(1)), (inc(2))
-- @expect: payload Insert
-- @json: 3

INSERT INTO Item SELECT inc(N) FROM SERIES(2)
-- @expect: payload Insert
-- @json: 2

UPDATE Item SET x = inc(x) WHERE x = inc(1)
-- @expect: payload Update
-- @json: 2

DELETE FROM Item WHERE inc(x) = 4
-- @expect: payload Delete
-- @json: 4

CREATE TABLE Result AS SELECT inc(x) AS y FROM Item
-- @expect: payload Create

SELECT y FROM Result
-- @expect:
-- | y: I64 |
-- | ------ |
-- | 2      |

-- @name: INSERT VALUES order and bounds preserve column defaults
CREATE TABLE OrderedValues (x INT, fallback INT DEFAULT 7)
-- @expect: payload Create

INSERT INTO OrderedValues (x) VALUES (inc(0)), (inc(1)), (inc(2)) ORDER BY inc(column1) DESC LIMIT inc(0) OFFSET inc(0)
-- @expect: payload Insert
-- @json: 1

SELECT x, fallback FROM OrderedValues
-- @expect:
-- | x: I64 | fallback: I64 |
-- | ------ | ------------- |
-- | 2      | 7             |

-- @name: schemaless INSERT VALUES executes prepared functions
CREATE FUNCTION document() RETURN '{"x": 2}'
-- @expect: payload Create

CREATE TABLE FunctionDocs
-- @expect: payload Create

INSERT INTO FunctionDocs VALUES (document())
-- @expect: payload Insert
-- @json: 1

SELECT x FROM FunctionDocs
-- @expect:
-- | x: I64 |
-- | ------ |
-- | 2      |

-- @name: CTAS VALUES executes prepared functions
CREATE TABLE FunctionValues AS VALUES (inc(0))
-- @expect: payload Create

SELECT column1 FROM FunctionValues
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 1            |
