CREATE TABLE Sale (
    id INTEGER,
    region TEXT,
    amount INTEGER
);
-- @expect: ok

INSERT INTO Sale (id, region, amount) VALUES
    (1, 'East', 100),
    (2, 'East', 300),
    (3, 'East', 200),
    (4, 'West', 150),
    (5, 'West', 100);
-- @expect: ok

SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM Sale ORDER BY id
-- @expect:
-- | id: I64 | rn: I64 |
-- | ------- | ------- |
-- | 1       | 3       |
-- | 2       | 1       |
-- | 3       | 2       |
-- | 4       | 1       |
-- | 5       | 2       |

SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS rn FROM Sale ORDER BY id
-- @expect:
-- | id: I64 | rn: I64 |
-- | ------- | ------- |
-- | 1       | 5       |
-- | 2       | 4       |
-- | 3       | 3       |
-- | 4       | 2       |
-- | 5       | 1       |

SELECT id, ROW_NUMBER() OVER (ORDER BY id) + 10 AS x FROM Sale ORDER BY id
-- @expect:
-- | id: I64 | x: I64 |
-- | ------- | ------ |
-- | 1       | 11     |
-- | 2       | 12     |
-- | 3       | 13     |
-- | 4       | 14     |
-- | 5       | 15     |
