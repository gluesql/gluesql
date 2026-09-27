SELECT ACOS(0.5) AS acos1, ACOS(1) AS acos2
-- @expect:
-- | acos1: F64     | acos2: F64 |
-- | -------------- | ---------- |
-- | 1.047197551197 | 0.0        |

SELECT ACOS('string') AS acos
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ACOS"

SELECT ACOS(null) AS acos
-- @expect:
-- | acos |
-- | ---- |
-- | NULL |

SELECT ACOS(true) AS acos
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ACOS"

SELECT ACOS() AS acos
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "ACOS"
-- }

SELECT ACOS(1.0, 2.0) AS acos
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "ACOS"
-- }
