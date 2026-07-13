-- name: DEDUP(CAST('[1, 2, 3, 3, 4, 5, 5]' AS List)) should return '[1, 2, 3, 4, 5]'
SELECT DEDUP(CAST('[1, 2, 3, 3, 4, 5, 5]' AS List)) as actual

-- expect:
-- | actual: List |
-- | [1,2,3,4,5]  |

-- name: DEDUP(CAST('['1', 1, '1']' AS List)) should return '['1', 1]'
SELECT DEDUP(CAST('["1", 1, 1, "1", "1"]' AS List)) as actual

-- expect:
-- | actual: List |
-- | ["1",1,"1"]  |

-- name: DEDUP with invalid value should return EvaluateError::ListTypeRequired
SELECT DEDUP(1) AS actual

-- expect: error Evaluate.ListTypeRequired
