CREATE TABLE Sale (
    id INTEGER,
    region TEXT,
    amount INTEGER
);
-- @expect: ok

INSERT INTO Sale (id, region, amount) VALUES
    (1, 'East', 100),
    (2, 'East', 200),
    (3, 'East', 300),
    (4, 'West', 500);
-- @expect: ok

SELECT
    id,
    LAG(amount) OVER (PARTITION BY region ORDER BY id) AS lg,
    LEAD(amount, 2, 0) OVER (PARTITION BY region ORDER BY id) AS ld2,
    LAG(amount, 1, -1) OVER (PARTITION BY region ORDER BY id) AS lgd
FROM Sale ORDER BY id
-- @expect:
-- | id: I64 | lg       | ld2: I64 | lgd: I64 |
-- | ------- | -------- | -------- | -------- |
-- | 1       | NULL     | 300      | -1       |
-- | 2       | I64(100) | 0        | 100      |
-- | 3       | I64(200) | 0        | 200      |
-- | 4       | NULL     | 0        | -1       |

SELECT id, LEAD(amount) OVER (ORDER BY id) AS ld FROM Sale ORDER BY id
-- @expect:
-- | id: I64 | ld       |
-- | ------- | -------- |
-- | 1       | I64(200) |
-- | 2       | I64(300) |
-- | 3       | I64(500) |
-- | 4       | NULL     |
