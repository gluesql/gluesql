CREATE TABLE ListTable (
    id INTEGER,
    items LIST
);
-- expect: ok

INSERT INTO ListTable VALUES
    (1, '[1, 2, 3]'),
    (2, '["1", "2", "3"]'),
    (3, '["1", 2, 3]')
-- expect: ok

-- name: SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3) should return '[1, 4, 5]'
SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3) AS actual
-- expect:
-- | actual: List |
-- | [1,4,5]      |

-- name: SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3, CAST('[100, 99]' AS List)) should return '[1, 100, 99, 4, 5]'
SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3, CAST('[100, 99]' AS List)) AS actual
-- expect:
-- | actual: List   |
-- | [1,100,99,4,5] |

-- name: SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) should return '[100, 99, 3]'
SELECT SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) AS actual
-- expect:
-- | actual: List |
-- | [100,99,3]   |

-- name: SPLICE(CAST('[1, 2, 3]' AS List), 1, 100, CAST('[100, 99]' AS List)) should return '[1, 100, 99]')
SELECT SPLICE(CAST('[1, 2, 3]' AS List), 1, 100, CAST('[100, 99]' AS List)) AS actual
-- expect:
-- | actual: List |
-- | [1,100,99]   |

-- name: SPLICE(3, 1, 2) sholud return EvaluateError::ListTypeRequired
SELECT SPLICE(1, 2, 3) AS actual
-- expect: error Evaluate.ListTypeRequired

-- name: SPLICE(CAST('[1, 2, 3]' AS List), 2, 4, 9) should return EvaluateError::ListTypeRequired
SELECT SPLICE(CAST('[1, 2, 3]' AS List), 2, 4, 9), AS actual
-- expect: error Evaluate.ListTypeRequired
