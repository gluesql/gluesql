CREATE FUNCTION inc(x INT) RETURN x + 1;
-- @expect: payload Create

CREATE FUNCTION shadow_argument(x INT) RETURN (
    SELECT x FROM (VALUES (9)) v(x)
);
-- @expect: payload Create

-- @name: A local column takes precedence over a function argument with the same name
SELECT shadow_argument(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 9      |

CREATE FUNCTION order_with_argument(x INT) RETURN (
    SELECT column1 FROM (VALUES (x), (x + 1)) v
    ORDER BY column1 + x DESC LIMIT 1
);
-- @expect: payload Create

-- @name: ORDER BY can use both a query column and a function argument
SELECT order_with_argument(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 5      |

CREATE FUNCTION nested_scopes(x INT) RETURN (
    SELECT (SELECT inc(v.n + x) FROM SERIES(v.n) LIMIT 1)
    FROM (VALUES (x + 1)) v(n)
);
-- @expect: payload Create

-- @name: A nested subquery can use an outer query column and a function argument
SELECT nested_scopes(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 10     |

CREATE FUNCTION joined_scopes(x INT) RETURN (
    SELECT SUM(b.N) FROM SERIES(x) a
    JOIN SERIES(inc(a.N)) b ON a.N = b.N
);
-- @expect: payload Create

-- @name: A joined source can use the current row while the body uses its arguments
SELECT joined_scopes(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 10     |

-- @name: Call arguments in an ordinary subquery can reference its outer row
SELECT (SELECT inc(v.n) FROM SERIES(v.n) LIMIT 1) AS r
FROM (VALUES (2), (3)) v(n) ORDER BY r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 3      |
-- | 4      |

CREATE FUNCTION isolated() RETURN x;
-- @expect: payload Create

-- @name: A function body does not inherit the caller row
SELECT isolated() FROM (VALUES (9)) v(x);
-- @expect: error Evaluate.IdentifierNotFound
-- @json: "x"
