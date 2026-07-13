CREATE TABLE Item (name TEXT DEFAULT REPEAT('hello', 2))
-- expect: payload Create

INSERT INTO Item VALUES ('hello')
-- expect: payload Insert
-- 1

SELECT REPEAT(name, 2) AS test FROM Item
-- expect:
-- | test: Str    |
-- | "hellohello" |

SELECT REPEAT('abcd') AS test FROM Item
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 2,
--   "found": 1,
--   "name": "REPEAT"
-- }

SELECT REPEAT('abcd', 2, 2) AS test FROM Item
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 2,
--   "found": 3,
--   "name": "REPEAT"
-- }

SELECT REPEAT(1, 1) AS test FROM Item
-- expect: error Evaluate.FunctionRequiresStringValue
-- "REPEAT"

SELECT REPEAT(name, null) AS test FROM Item
-- expect:
-- | test |
-- | NULL |

CREATE TABLE NullTest (name TEXT null)
-- expect: payload Create

INSERT INTO NullTest VALUES (null)
-- expect: payload Insert
-- 1

SELECT REPEAT(name, 2) AS test FROM NullTest
-- expect:
-- | test |
-- | NULL |
