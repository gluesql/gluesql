SELECT ASIN(0.5) AS asin1, ASIN(1) AS asin2
-- @expect:
-- | asin1: F64     | asin2: F64     |
-- | -------------- | -------------- |
-- | 0.523598775598 | 1.570796326795 |

SELECT ASIN('string') AS asin
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ASIN"

SELECT ASIN(null) AS asin
-- @expect:
-- | asin |
-- | ---- |
-- | NULL |

SELECT ASIN() AS asin
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "ASIN"
-- }

SELECT ASIN(1.0, 2.0) AS sin
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "ASIN"
-- }
