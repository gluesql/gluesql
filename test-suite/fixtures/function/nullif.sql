-- @name: NULLIF with equal integers should return NULL
SELECT NULLIF(0, 0) AS result
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

-- @name: NULLIF with different integers should return first arguments
SELECT NULLIF(1, 0) AS result
-- @expect:
-- | result: I64 |
-- | ----------- |
-- | 1           |

-- @name: NULLIF with equal strings should return NULL
SELECT NULLIF('hello', 'hello') AS result
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

-- @name: NULLIF with different strings should return first arguments
SELECT NULLIF('hello', 'helle') AS result
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "hello"     |

-- @name: NULLIF with equal date should return NULL
SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-01', '%Y-%m-%d')) AS result
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

-- @name: NULLIF with different date should return first arguments
SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-02', '%Y-%m-%d')) AS result
-- @expect:
-- | result: Date |
-- | ------------ |
-- | "2025-01-01" |

-- @name: NULLIF with zero argument should throw EvaluateError
SELECT NULLIF() AS result
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 2,
--   "found": 0,
--   "name": "NULLIF"
-- }

-- @name: NULLIF with one argument should throw EvaluateError
SELECT NULLIF(1) AS result
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 2,
--   "found": 1,
--   "name": "NULLIF"
-- }
