SELECT PI() AS pi

-- expect:
-- | pi: F64           |
-- | 3.141592653589793 |

SELECT PI(0) AS pi

-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 0,
--   "found": 1,
--   "name": "PI"
-- }
