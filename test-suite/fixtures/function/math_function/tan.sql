SELECT TAN(0.5) AS tan1, TAN(1) AS tan2
-- expect:
-- | tan1: F64      | tan2: F64      |
-- | 0.546302489844 | 1.557407724655 |

SELECT TAN(null) AS tan
-- expect:
-- | tan  |
-- | NULL |

SELECT TAN(true) AS tan
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "TAN"

SELECT TAN(false) AS tan
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "TAN"

SELECT TAN('string') AS tan
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "TAN"

SELECT TAN() AS tan
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "TAN"
-- }

SELECT TAN(1.0, 2.0) AS tan
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "TAN"
-- }
