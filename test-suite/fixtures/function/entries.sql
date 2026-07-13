-- name: test entries function works while creating a table simultaneously
CREATE TABLE Item (map MAP)

-- expect: payload Create

-- name: test if the sample string gets inserted to table
INSERT INTO Item VALUES ('{"name":"GlueSQL"}')

-- expect: payload Insert
-- 1

-- name: check id the entries function works with the previously inserted string
SELECT ENTRIES(map) AS test FROM Item

-- expect:
-- | test: List           |
-- | [["name","GlueSQL"]] |

-- name: test ENTRIES function requires map value
SELECT ENTRIES(1) FROM Item

-- expect: error Evaluate.FunctionRequiresMapValue
-- "ENTRIES"
