SELECT COS(0.5) AS cos1, COS(1) AS cos2
-- @expect:
-- | cos1: F64      | cos2: F64      |
-- | 0.877582561890 | 0.540302305868 |

SELECT COS(null) AS cos
-- @expect:
-- | cos  |
-- | NULL |

SELECT COS(true) AS cos
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "COS"

SELECT COS(false) AS cos
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "COS"

SELECT COS('string') AS cos
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "COS"

SELECT COS() AS cos
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "COS"
-- }

SELECT COS(1.0, 2.0) AS cos
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "COS"
-- }
