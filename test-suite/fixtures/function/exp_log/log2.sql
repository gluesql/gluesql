SELECT
    LOG2(64.0) as log2_1,
    LOG2(0.04) as log2_2
    ;
-- @expect:
-- | log2_1: F64 | log2_2: F64     |
-- | ----------- | --------------- |
-- | 6.0         | -4.643856189775 |

SELECT LOG2(32) as log2_with_int;
-- @expect:
-- | log2_with_int: F64 |
-- | ------------------ |
-- | 5.0                |

SELECT LOG2('string') AS log2;
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "LOG2"

SELECT LOG2(NULL) AS log2
-- @expect:
-- | log2 |
-- | ---- |
-- | NULL |
