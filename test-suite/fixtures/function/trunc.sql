-- @name: TRUNC(0.3) should return 0.0
SELECT TRUNC(0.3) as actual
-- @expect:
-- | actual: F64 |
-- | ----------- |
-- | 0.0         |

-- @name: TRUNC(-0.8) should return -0.0 (truncate toward zero)
SELECT TRUNC(-0.8) as actual
-- @expect:
-- | actual: F64 |
-- | ----------- |
-- | -0.0        |

-- @name: TRUNC(10) should return 10.0 (integer unchanged)
SELECT TRUNC(10) as actual
-- @expect:
-- | actual: F64 |
-- | ----------- |
-- | 10.0        |

-- @name: TRUNC(6.87421) should return 6.0
SELECT TRUNC(6.87421) as actual
-- @expect:
-- | actual: F64 |
-- | ----------- |
-- | 6.0         |

-- @name: TRUNC(-3.7) should return -3.0 (truncate toward zero)
SELECT TRUNC(-3.7) as actual
-- @expect:
-- | actual: F64 |
-- | ----------- |
-- | -3.0        |

-- @name: TRUNC with string should return EvaluateError::FunctionRequiresFloatValue
SELECT TRUNC('string') AS actual
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "TRUNC"

-- @name: TRUNC with boolean should return EvaluateError::FunctionRequiresFloatValue
SELECT TRUNC(TRUE) AS actual
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "TRUNC"

-- @name: TRUNC with NULL should return NULL
SELECT TRUNC(NULL) AS actual
-- @expect:
-- | actual |
-- | ------ |
-- | NULL   |
