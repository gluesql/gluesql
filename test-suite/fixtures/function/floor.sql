SELECT
    FLOOR(0.3) as floor1,
    FLOOR(-0.8) as floor2,
    FLOOR(10) as floor3,
    FLOOR(6.87421) as floor4
    ;
-- expect:
-- | floor1: F64 | floor2: F64 | floor3: F64 | floor4: F64 |
-- | 0.0         | -1.0        | 10.0        | 6.0         |

SELECT FLOOR('string') AS floor
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "FLOOR"

SELECT FLOOR(NULL) AS floor
-- expect:
-- | floor |
-- | NULL  |

SELECT FLOOR(TRUE) AS floor
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "FLOOR"

SELECT FLOOR(FALSE) AS floor
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "FLOOR"

SELECT FLOOR('string' TO DAY) AS floor
-- expect: error Translate.UnsupportedExpr
-- "FLOOR('string' TO DAY)"
