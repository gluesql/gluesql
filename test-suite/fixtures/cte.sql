WITH t AS (SELECT 1 AS n) SELECT * FROM t
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 1      |

WITH t AS (SELECT 2 AS n) SELECT t.n FROM t
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 2      |

WITH t AS (SELECT 3 AS n) SELECT x.n FROM t AS x
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 3      |

WITH t AS (SELECT 4 AS n) SELECT x.n AS lhs, y.n AS rhs FROM t AS x JOIN t AS y ON x.n = y.n
-- @expect:
-- | lhs: I64 | rhs: I64 |
-- | -------- | -------- |
-- | 4        | 4        |

WITH a AS (SELECT 5 AS n), b AS (SELECT n + 1 AS n FROM a) SELECT b.n FROM b
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 6      |

WITH a AS (SELECT 7 AS n) SELECT x.n FROM (SELECT * FROM a) AS x
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 7      |

WITH a AS (SELECT 8 AS n) SELECT (SELECT n FROM a) AS n
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 8      |

WITH a AS (SELECT 9 AS n) SELECT n FROM a WHERE EXISTS (SELECT * FROM a) AND n IN (SELECT n FROM a)
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 9      |

CREATE TABLE collision (n INTEGER)
-- @expect: payload Create

INSERT INTO collision VALUES (10)
-- @expect: payload Insert
-- @json: 1

WITH collision AS (SELECT 11 AS n) SELECT collision.n FROM collision
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 11     |

WITH collision AS (SELECT n + 1 AS n FROM collision) SELECT * FROM collision
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 11     |

CREATE TABLE later (n INTEGER)
-- @expect: payload Create

INSERT INTO later VALUES (12)
-- @expect: payload Insert
-- @json: 1

WITH earlier AS (SELECT * FROM later), later AS (SELECT 99 AS n) SELECT * FROM earlier
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 12     |

WITH a AS (SELECT 13 AS n) SELECT x.n AS inner_n, a.n AS outer_n FROM (WITH a AS (SELECT n + 1 AS n FROM a) SELECT * FROM a) AS x JOIN a
-- @expect:
-- | inner_n: I64 | outer_n: I64 |
-- | ------------ | ------------ |
-- | 14           | 13           |

WITH t AS (VALUES (3), (1), (2)) SELECT * FROM t ORDER BY column1 LIMIT 1 OFFSET 1
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 2            |

WITH t AS (SELECT 1 AS n) VALUES (1), (2) ORDER BY column1 DESC LIMIT 1
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 2            |

WITH t AS (SELECT 1 AS n) VALUES ((SELECT n FROM t))
-- @expect: error Evaluate.SubqueryNotAllowedInStatelessExpr

WITH t AS (SELECT 1 AS n), t AS (SELECT 2 AS n) SELECT * FROM t
-- @expect: error Planner.DuplicateCteName
-- @json: "t"

WITH t AS (SELECT * FROM t) SELECT * FROM t
-- @expect: error Fetch.TableNotFound
-- @json: "t"

WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS n) SELECT * FROM a
-- @expect: error Fetch.TableNotFound
-- @json: "b"

WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t
-- @expect: error Translate.UnsupportedCteOption
-- @json: "WITH RECURSIVE"

WITH t AS MATERIALIZED (SELECT 1) SELECT * FROM t
-- @expect: error Translate.UnsupportedCteOption
-- @json: "MATERIALIZED"

WITH t AS NOT MATERIALIZED (SELECT 1) SELECT * FROM t
-- @expect: error Translate.UnsupportedCteOption
-- @json: "NOT MATERIALIZED"

WITH t AS (SELECT 1) FROM source SELECT * FROM t
-- @expect: error Translate.UnsupportedCteOption
-- @json: "CTE FROM"

WITH t(n) AS (SELECT 1) SELECT * FROM t
-- @expect: error Translate.UnsupportedCteOption
-- @json: "CTE column alias list"

WITH t AS (SELECT 15 AS n) SELECT t.* FROM t
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 15     |

WITH t AS (SELECT 16 AS n) SELECT x.renamed FROM t AS x(renamed)
-- @expect:
-- | renamed: I64 |
-- | ------------ |
-- | 16           |

WITH t AS (VALUES (1), (1), (2)) SELECT DISTINCT column1 FROM t ORDER BY column1
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 1            |
-- | 2            |

WITH t AS (VALUES (1), (2)) SELECT COUNT(*) AS n FROM t GROUP BY column1 HAVING COUNT(*) > 0 ORDER BY column1
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 1      |
-- | 1      |

CREATE TABLE CteResult AS WITH t AS (SELECT 21 AS n) SELECT * FROM t
-- @expect: payload Create

SELECT * FROM CteResult
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 21     |

INSERT INTO collision WITH t AS (SELECT 22 AS n) SELECT * FROM t
-- @expect: payload Insert
-- @json: 1

UPDATE collision SET n = (WITH t AS (SELECT 23 AS n) SELECT n FROM t) WHERE n IN (WITH t AS (SELECT 22 AS n) SELECT n FROM t)
-- @expect: payload Update
-- @json: 1

SELECT * FROM collision ORDER BY n
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 10     |
-- | 23     |

DELETE FROM collision WHERE n IN (WITH t AS (SELECT 23 AS n) SELECT n FROM t)
-- @expect: payload Delete
-- @json: 1

SELECT * FROM collision
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 10     |
