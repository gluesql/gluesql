SELECT
        CEIL(0.3) AS ceil1,
        CEIL(-0.8) AS ceil2,
        CEIL(10) AS ceil3,
        CEIL(6.87421) AS ceil4
    ;
-- expect:
-- | ceil1: F64 | ceil2: F64 | ceil3: F64 | ceil4: F64 |
-- | 1.0        | 0.0        | 10.0       | 7.0        |

SELECT CEIL('string') AS ceil;
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "CEIL"

SELECT CEIL(NULL) AS ceil;
-- expect:
-- | ceil |
-- | NULL |

SELECT CEIL(TRUE) AS ceil;
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "CEIL"

SELECT CEIL(FALSE) AS ceil;
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "CEIL"

SELECT CEIL('string' TO DAY) AS ceil;
-- expect: error Translate.UnsupportedExpr
-- "CEIL('string' TO DAY)"
