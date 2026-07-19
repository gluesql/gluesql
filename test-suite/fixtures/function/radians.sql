SELECT
    RADIANS(180.0) as radians_1,
    RADIANS(360.0) as radians_2
    ;
-- @expect:
-- | radians_1: F64 | radians_2: F64 |
-- | 3.14159265359  | 6.28318530718  |

SELECT RADIANS(90) as radians_with_int
-- @expect:
-- | radians_with_int: F64 |
-- | 1.570796326795        |

SELECT RADIANS(0) as radians_with_zero
-- @expect:
-- | radians_with_zero: F64 |
-- | 0.0                    |

SELECT RADIANS(-900) as radians_with_zero
-- @expect:
-- | radians_with_zero: F64 |
-- | -15.707963267949       |

SELECT RADIANS(900) as radians_with_zero
-- @expect:
-- | radians_with_zero: F64 |
-- | 15.707963267949        |

SELECT RADIANS(DEGREES(90)) as degrees_to_radians
-- @expect:
-- | degrees_to_radians: F64 |
-- | 90.0                    |

SELECT RADIANS(0, 0) as radians_arg2
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "RADIANS"
-- }

SELECT RADIANS() as radians_arg0
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "RADIANS"
-- }

SELECT RADIANS('string') AS radians
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "RADIANS"

SELECT RADIANS(NULL) AS radians
-- @expect:
-- | radians |
-- | NULL    |
