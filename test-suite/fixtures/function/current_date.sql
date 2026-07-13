-- name: table with CURRENT_DATE default
CREATE TABLE Item (date DATE DEFAULT CURRENT_DATE)
-- expect: payload Create

-- name: insert date values
INSERT INTO Item VALUES
    ('2021-06-15'),
    ('9999-12-31');
-- expect: payload Insert
-- 2

-- name: filter by CURRENT_DATE
SELECT date FROM Item WHERE date > CURRENT_DATE;
-- expect:
-- | date: Date   |
-- | "9999-12-31" |
