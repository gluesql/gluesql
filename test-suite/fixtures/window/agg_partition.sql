CREATE TABLE Sale (
    id INTEGER,
    region TEXT,
    amount INTEGER NULL
);
-- @expect: ok

INSERT INTO Sale (id, region, amount) VALUES
    (1, 'East', 100),
    (2, 'East', 300),
    (3, 'East', NULL),
    (4, 'West', 150),
    (5, 'West', 100);
-- @expect: ok

SELECT
    id,
    SUM(amount) OVER (PARTITION BY region) AS s,
    COUNT(amount) OVER (PARTITION BY region) AS c,
    COUNT(*) OVER (PARTITION BY region) AS cw,
    MIN(amount) OVER (PARTITION BY region) AS mn,
    MAX(amount) OVER (PARTITION BY region) AS mx,
    AVG(amount) OVER (PARTITION BY region) AS a
FROM Sale ORDER BY id
-- @expect:
-- | id: I64 | s: I64 | c: I64 | cw: I64 | mn: I64 | mx: I64 | a: F64 |
-- | ------- | ------ | ------ | ------- | ------- | ------- | ------ |
-- | 1       | 400    | 2      | 3       | 100     | 300     | 200.0  |
-- | 2       | 400    | 2      | 3       | 100     | 300     | 200.0  |
-- | 3       | 400    | 2      | 3       | 100     | 300     | 200.0  |
-- | 4       | 250    | 2      | 2       | 100     | 150     | 125.0  |
-- | 5       | 250    | 2      | 2       | 100     | 150     | 125.0  |
