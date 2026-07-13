SELECT
        DEGREES(PI() / 2) as degrees_1,
        DEGREES(PI()) as degrees_2
    ;
-- expect:
-- | degrees_1: F64 | degrees_2: F64 |
-- | 90.0           | 180.0          |

SELECT DEGREES(PI() / 2) as degrees_with_int;
-- expect:
-- | degrees_with_int: F64 |
-- | 90.0                  |

SELECT DEGREES(0) as degrees_with_zero;
-- expect:
-- | degrees_with_zero: F64 |
-- | 0.0                    |

SELECT DEGREES(RADIANS(90)) as radians_to_degrees;
-- expect:
-- | radians_to_degrees: F64 |
-- | 90.0                    |

SELECT DEGREES(0, 0) as degrees_arg2;
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "DEGREES"
-- }

SELECT DEGREES() as degrees_arg0;
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "DEGREES"
-- }

SELECT DEGREES('string') AS degrees;
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "DEGREES"

SELECT DEGREES(NULL) AS degrees;
-- expect:
-- | degrees |
-- | NULL    |
