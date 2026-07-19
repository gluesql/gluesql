CREATE TABLE Food (name Text null)
-- @expect: payload Create

INSERT INTO Food VALUES ('pork')
-- @expect: payload Insert
-- @json: 1

INSERT INTO Food VALUES ('burger')
-- @expect: payload Insert
-- @json: 1

SELECT POSITION('e' IN name) AS test FROM Food
-- @expect:
-- | test: I64 |
-- | --------- |
-- | 0         |
-- | 5         |

SELECT POSITION('s' IN 'cheese') AS test
-- @expect:
-- | test: I64 |
-- | --------- |
-- | 5         |

SELECT POSITION(NULL IN 'cheese') AS test
-- @expect:
-- | test |
-- | ---- |
-- | NULL |

SELECT POSITION(1 IN 'cheese') AS test
-- @expect: error Value.NonStringParameterInPosition
-- @json:
-- {
--   "from": {
--     "Str": "cheese"
--   },
--   "sub": {
--     "I64": 1
--   }
-- }
