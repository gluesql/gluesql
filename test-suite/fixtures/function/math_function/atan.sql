CREATE TABLE SingleItem (id INTEGER DEFAULT ATAN(3.14))
-- expect: payload Create

INSERT INTO SingleItem VALUES (0)
-- expect: payload Insert
-- 1

SELECT ATAN(0.5) AS atan1, ATAN(1) AS atan2
-- expect:
-- | atan1: F64     | atan2: F64     |
-- | 0.463647609001 | 0.785398163397 |

SELECT ATAN('string') AS atan
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "ATAN"

SELECT ATAN(null) AS atan
-- expect:
-- | atan |
-- | NULL |

SELECT ATAN(true) AS atan
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "ATAN"

SELECT ATAN() AS atan
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "ATAN"
-- }

SELECT ATAN(1.0, 2.0) AS atan
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "ATAN"
-- }
