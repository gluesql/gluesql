CREATE TABLE SingleItem (qty Float DEFAULT ROUND(RAND()*100))
-- expect: payload Create

INSERT INTO SingleItem VALUES (ROUND(RAND(1)*100))
-- expect: payload Insert
-- 1

SELECT RAND(123) AS rand1, RAND(789.0) AS rand2
-- expect:
-- | rand1: F64     | rand2: F64     |
-- | 0.173254644262 | 0.963521823401 |

SELECT RAND('string') AS rand
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "RAND"

SELECT RAND(NULL) AS rand
-- expect:
-- | rand |
-- | NULL |

SELECT RAND(TRUE) AS rand
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "RAND"

SELECT RAND(FALSE) AS rand
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "RAND"

SELECT RAND('string', 'string2') AS rand
-- expect: error Translate.FunctionArgsLengthNotWithinRange
-- {
--   "expected_maximum": 1,
--   "expected_minimum": 0,
--   "found": 2,
--   "name": "RAND"
-- }
