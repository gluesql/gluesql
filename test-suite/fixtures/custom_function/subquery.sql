CREATE FUNCTION inc(x INT) RETURN x + 1;
-- @expect: payload Create

CREATE FUNCTION scalar_query(x INT) RETURN (SELECT inc(x));
-- @expect: payload Create

-- @name: A scalar subquery can call another custom function
SELECT scalar_query(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 5      |

CREATE FUNCTION count_series(x INT) RETURN (
    SELECT COUNT(*) FROM SERIES(x) WHERE N <= x
);
-- @expect: payload Create

-- @name: A function body can filter and aggregate SERIES rows
SELECT count_series(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 4      |

CREATE FUNCTION sum_increments(x INT) RETURN (
    SELECT SUM(inc(N)) FROM SERIES(x)
);
-- @expect: payload Create

-- @name: An aggregate can contain custom function calls
SELECT sum_increments(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 14     |

CREATE FUNCTION value_query(x INT) RETURN (
    SELECT column1 FROM (VALUES (inc(x))) v
);
-- @expect: payload Create

-- @name: A function body can query VALUES containing a custom function call
SELECT value_query(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 5      |

CREATE FUNCTION sum_values(x INT) RETURN (
    SELECT SUM(v.column1) FROM (VALUES (x), (inc(x))) v
);
-- @expect: payload Create

-- @name: A function body can aggregate a derived VALUES source
SELECT sum_values(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 9      |

CREATE FUNCTION ordered_series(x INT) RETURN (
    SELECT N FROM SERIES(x + 2) ORDER BY N DESC LIMIT 1 OFFSET x
);
-- @expect: payload Create

-- @name: A function body can order rows and select one with OFFSET
SELECT ordered_series(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 2      |

CREATE FUNCTION limited_series(x INT) RETURN (
    SELECT N FROM SERIES(3) LIMIT x - 3 OFFSET x - 3
);
-- @expect: payload Create

-- @name: Function arguments can determine LIMIT and OFFSET
SELECT limited_series(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 2      |
