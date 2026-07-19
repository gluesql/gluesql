-- @name: table with CURRENT_TIME default
CREATE TABLE Item (time TIME DEFAULT CURRENT_TIME)
-- @expect: payload Create

-- @name: insert time values
INSERT INTO Item VALUES
    ('06:42:40'),
    ('23:59:59');
-- @expect: payload Insert
-- @json: 2

-- @name: CURRENT_TIME is not null
SELECT CURRENT_TIME IS NOT NULL as is_not_null
-- @expect:
-- | is_not_null: Bool |
-- | true              |

-- @name: CURRENT_TIME in valid range
SELECT CURRENT_TIME >= TIME '00:00:00' AND CURRENT_TIME <= TIME '23:59:59' as is_valid_range
-- @expect:
-- | is_valid_range: Bool |
-- | true                 |
