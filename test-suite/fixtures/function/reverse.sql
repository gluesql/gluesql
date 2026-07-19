CREATE TABLE Item (name TEXT DEFAULT REVERSE('world'))
-- @expect: payload Create

INSERT INTO Item VALUES ('Let''s meet')
-- @expect: payload Insert
-- @json: 1

SELECT REVERSE(name) AS test FROM Item;
-- @expect:
-- | test: Str    |
-- | "teem s'teL" |

SELECT REVERSE(1) AS test FROM Item
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "REVERSE"

CREATE TABLE NullTest (name TEXT null)
-- @expect: payload Create

INSERT INTO NullTest VALUES (null)
-- @expect: payload Insert
-- @json: 1

SELECT REVERSE(name) AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |
