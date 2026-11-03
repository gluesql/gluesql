CREATE FUNCTION total(x INT) RETURN
    CASE WHEN x <= 0 THEN 0 ELSE x + total(x - 1) END;
-- @expect: payload Create

-- @name: A function can call itself until its base case
SELECT total(5) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 15     |

CREATE FUNCTION even_value(x INT) RETURN
    CASE WHEN x = 0 THEN true ELSE odd_value(x - 1) END;
-- @expect: payload Create

CREATE FUNCTION odd_value(x INT) RETURN
    CASE WHEN x = 0 THEN false ELSE even_value(x - 1) END;
-- @expect: payload Create

-- @name: Functions can call each other recursively
SELECT even_value(4) AS r;
-- @expect:
-- | r: Bool |
-- | ------- |
-- | true    |
