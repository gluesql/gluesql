SELECT SIN(PI() / 6) AS sin1, SIN(0) AS sin2
-- @expect:
-- | sin1: F64 | sin2: F64 |
-- | --------- | --------- |
-- | 0.5       | 0.0       |

SELECT SIN(null) AS sin
-- @expect:
-- | sin  |
-- | ---- |
-- | NULL |

SELECT SIN(true) AS sin
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "SIN"

SELECT SIN(false) AS sin
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "SIN"

SELECT SIN('string') AS sin
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "SIN"

SELECT SIN() AS sin
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "SIN"
-- }

SELECT SIN(1.0, 2.0) AS sin
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "SIN"
-- }
