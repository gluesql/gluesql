SELECT TAN(PI() / 4) AS tan1, TAN(0) AS tan2
-- @expect:
-- | tan1: F64 | tan2: F64 |
-- | --------- | --------- |
-- | 1.0       | 0.0       |

SELECT TAN(null) AS tan
-- @expect:
-- | tan  |
-- | ---- |
-- | NULL |

SELECT TAN(true) AS tan
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "TAN"

SELECT TAN(false) AS tan
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "TAN"

SELECT TAN('string') AS tan
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "TAN"

SELECT TAN() AS tan
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "TAN"
-- }

SELECT TAN(1.0, 2.0) AS tan
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "TAN"
-- }
