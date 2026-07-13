SELECT ASIN(0.5) AS asin1, ASIN(1) AS asin2
-- expect:
-- | asin1: F64         | asin2: F64         |
-- | 0.5235987755982988 | 1.5707963267948966 |

SELECT ASIN('string') AS asin
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "ASIN"

SELECT ASIN(null) AS asin
-- expect:
-- | asin |
-- | NULL |

SELECT ASIN() AS asin
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "ASIN"
-- }

SELECT ASIN(1.0, 2.0) AS sin
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "ASIN"
-- }
