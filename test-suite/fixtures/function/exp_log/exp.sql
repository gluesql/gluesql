SELECT
        EXP(2.0) as exp1,
        EXP(5.5) as exp2
    ;
-- expect:
-- | exp1: F64        | exp2: F64          |
-- | 7.38905609893065 | 244.69193226422038 |

SELECT EXP(3) as exp_with_int;
-- expect:
-- | exp_with_int: F64  |
-- | 20.085536923187668 |

SELECT EXP('string') AS exp;
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "EXP"

SELECT EXP(NULL) AS exp
-- expect:
-- | exp  |
-- | NULL |
