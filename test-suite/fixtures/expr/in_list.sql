-- @name: 'NULL IN (...)' should return 'NULL'
SELECT NULL IN (1, 2, 3) as res
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: 'NULL IN (...)' should return 'NULL' even if the list includes 'NULL'
SELECT NULL IN (1, 2, 3, NULL) as res
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: a matching value determines IN regardless of NULL position
SELECT
    1 IN (1, NULL) AS match_first,
    1 IN (NULL, 1) AS match_last,
    1 NOT IN (NULL, 1) AS negated;
-- @expect:
-- | match_first: Bool | match_last: Bool | negated: Bool |
-- | ----------------- | ---------------- | ------------- |
-- | true              | true             | false         |

-- @name: an unmatched value is unknown when the list contains NULL
SELECT
    2 IN (1, NULL) AS included,
    2 NOT IN (1, NULL) AS excluded;
-- @expect:
-- | included | excluded |
-- | -------- | -------- |
-- | NULL     | NULL     |

-- @name: an unmatched value is FALSE when all list values are known
SELECT
    2 IN (1, 3) AS included,
    2 NOT IN (1, 3) AS excluded;
-- @expect:
-- | included: Bool | excluded: Bool |
-- | -------------- | -------------- |
-- | false          | true           |

-- @name: NOT IN preserves an unknown target
SELECT NULL NOT IN (1, 2, 3) AS res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |
