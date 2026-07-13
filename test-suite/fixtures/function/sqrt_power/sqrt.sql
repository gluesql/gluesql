SELECT
        SQRT(4.0) as sqrt_1,
        SQRT(0.07) as sqrt_2
    ;

-- expect:
-- | sqrt_1: F64 | sqrt_2: F64        |
-- | 2.0         | 0.2645751311064591 |

SELECT SQRT(64) as sqrt_with_int

-- expect:
-- | sqrt_with_int: F64 |
-- | 8.0                |

SELECT SQRT(0) as sqrt_with_zero

-- expect:
-- | sqrt_with_zero: F64 |
-- | 0.0                 |

SELECT SQRT('string') AS sqrt

-- expect: error Value.SqrtOnNonNumeric
-- {
--   "Str": "string"
-- }

SELECT SQRT(NULL) AS sqrt

-- expect:
-- | sqrt |
-- | NULL |
