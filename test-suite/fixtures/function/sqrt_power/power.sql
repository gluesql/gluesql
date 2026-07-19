SELECT
    POWER(2.0,4) as power_1,
    POWER(0.07,3) as power_2
    ;
-- @expect:
-- | power_1: F64 | power_2: F64 |
-- | ------------ | ------------ |
-- | 16.0         | 0.000343     |

SELECT
    POWER(0,4) as power_with_zero,
    POWER(3,0) as power_to_zero
    ;
-- @expect:
-- | power_with_zero: F64 | power_to_zero: F64 |
-- | -------------------- | ------------------ |
-- | 0.0                  | 1.0                |

SELECT POWER(32,3.0) as power_with_float
-- @expect:
-- | power_with_float: F64 |
-- | --------------------- |
-- | 32768.0               |

SELECT POWER('string','string') AS power
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "POWER"

SELECT POWER(2.0,'string') AS power
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "POWER"

SELECT POWER('string',2.0) AS power
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "POWER"

SELECT POWER(NULL,NULL) AS power
-- @expect:
-- | power |
-- | ----- |
-- | NULL  |

SELECT POWER(2.0,NULL) AS power
-- @expect:
-- | power |
-- | ----- |
-- | NULL  |

SELECT POWER(NULL,2.0) AS power
-- @expect:
-- | power |
-- | ----- |
-- | NULL  |
