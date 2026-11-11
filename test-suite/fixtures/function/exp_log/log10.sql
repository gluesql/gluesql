SELECT
    LOG10(100.0) as log10_1,
    LOG10(0.01) as log10_2
    ;
-- @expect:
-- | log10_1: F64 | log10_2: F64 |
-- | ------------ | ------------ |
-- | 2.0          | -2.0         |

SELECT LOG10(SQRT(10.0)) AS log10_fractional
-- @expect:
-- | log10_fractional: F64 |
-- | --------------------- |
-- | 0.5                   |

SELECT LOG10(10) as log10_with_int
-- @expect:
-- | log10_with_int: F64 |
-- | ------------------- |
-- | 1.0                 |

SELECT LOG10('string') AS log10
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "LOG10"

SELECT LOG10(NULL) AS log10
-- @expect:
-- | log10 |
-- | ----- |
-- | NULL  |
