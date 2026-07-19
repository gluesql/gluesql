SELECT
    ROUND(0.3) AS round1,
    ROUND(-0.8) AS round2,
    ROUND(10) AS round3,
    ROUND(6.87421) AS round4
    ;
-- @expect:
-- | round1: F64 | round2: F64 | round3: F64 | round4: F64 |
-- | 0.0         | -1.0        | 10.0        | 7.0         |

SELECT ROUND('string') AS round
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ROUND"

SELECT ROUND(NULL) AS round
-- @expect:
-- | round |
-- | NULL  |

SELECT ROUND(TRUE) AS round
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ROUND"

SELECT ROUND(FALSE) AS round
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ROUND"

SELECT ROUND('string', 'string2') AS round
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "ROUND"
-- }
