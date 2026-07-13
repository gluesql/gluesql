SELECT
        LOG10(64.0) as log10_1,
        LOG10(0.04) as log10_2
    ;
-- expect:
-- | log10_1: F64      | log10_2: F64        |
-- | 1.806179973983887 | -1.3979400086720375 |

SELECT LOG10(10) as log10_with_int
-- expect:
-- | log10_with_int: F64 |
-- | 1.0                 |

SELECT LOG10('string') AS log10
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "LOG10"

SELECT LOG10(NULL) AS log10
-- expect:
-- | log10 |
-- | NULL  |
