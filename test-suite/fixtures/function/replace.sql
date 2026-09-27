-- @name: test replace function works while creating a table simultaneously
CREATE TABLE Item (name TEXT DEFAULT REPLACE('SQL Tutorial', 'T', 'M'))
-- @expect: payload Create

-- @name: test if the sample string gets inserted to table
INSERT INTO Item VALUES ('Tticky GlueTQL')
-- @expect: payload Insert
-- @json: 1

-- @name: check id the replace function works with the previously inserted string
SELECT REPLACE(name,'T','S') AS test FROM Item;
-- @expect:
-- | test: Str        |
-- | ---------------- |
-- | "Sticky GlueSQL" |

-- @name: test when one argument was given
SELECT REPLACE('GlueSQL') AS test FROM Item
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 3,
--   "found": 1,
--   "name": "REPLACE"
-- }

-- @name: test when two arguments were given
SELECT REPLACE('GlueSQL','G') AS test FROM Item
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 3,
--   "found": 2,
--   "name": "REPLACE"
-- }

-- @name: test when integers were given as arguments instead of string values
SELECT REPLACE(1,1,1) AS test FROM Item
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "REPLACE"

-- @name: test when null was given as argument
SELECT REPLACE(name, null,null) AS test FROM Item
-- @expect:
-- | test |
-- | ---- |
-- | NULL |

-- @name: test if the table can be created will null value
CREATE TABLE NullTest (name TEXT null)
-- @expect: payload Create

-- @name: test if null can be inserted
INSERT INTO NullTest VALUES (null)
-- @expect: payload Insert
-- @json: 1

-- @name: test if replace works in null value
SELECT REPLACE(name, 'G','T') AS test FROM NullTest
-- @expect:
-- | test |
-- | ---- |
-- | NULL |

DELETE FROM Item
-- @expect: ok
