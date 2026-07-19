SELECT PI() AS pi
-- @expect:
-- | pi: F64       |
-- | ------------- |
-- | 3.14159265359 |

SELECT PI(0) AS pi
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 0,
--   "found": 1,
--   "name": "PI"
-- }
