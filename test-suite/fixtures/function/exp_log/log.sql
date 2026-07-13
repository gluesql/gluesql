SELECT
    LOG(64.0, 2.0) as log_1,
    LOG(0.04, 10.0) as log_2
    ;
-- expect:
-- | log_1: F64 | log_2: F64          |
-- | 6.0        | -1.3979400086720375 |

SELECT LOG(10, 10) as log_with_int
-- expect:
-- | log_with_int: F64 |
-- | 1.0               |

SELECT LOG('string', 10) AS log
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "LOG"

SELECT LOG(10, 'string') AS log
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "LOG"

SELECT LOG(NULL, 10) AS log
-- expect:
-- | log  |
-- | NULL |

SELECT LOG(10, NULL) AS log
-- expect:
-- | log  |
-- | NULL |
