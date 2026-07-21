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
