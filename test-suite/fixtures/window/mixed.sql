CREATE TABLE Sale (
    id INTEGER,
    region TEXT,
    amount INTEGER
);
-- @expect: ok

INSERT INTO Sale (id, region, amount) VALUES
    (1, 'East', 100),
    (2, 'East', 300),
    (3, 'West', 200);
-- @expect: ok

SELECT
    id,
    region,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn,
    SUM(amount) OVER () AS total,
    amount
FROM Sale ORDER BY id
-- @expect:
-- | id: I64 | region: Str | rn: I64 | total: I64 | amount: I64 |
-- | ------- | ----------- | ------- | ---------- | ----------- |
-- | 1       | "East"      | 1       | 600        | 100         |
-- | 2       | "East"      | 2       | 600        | 300         |
-- | 3       | "West"      | 1       | 600        | 200         |

SELECT id, RANK() OVER (ORDER BY amount DESC) AS r FROM Sale ORDER BY amount DESC LIMIT 2
-- @expect:
-- | id: I64 | r: I64 |
-- | ------- | ------ |
-- | 2       | 1      |
-- | 3       | 2      |

CREATE TABLE Region (name TEXT);
-- @expect: ok

INSERT INTO Region (name) VALUES ('East'), ('West');
-- @expect: ok

SELECT s.id, ROW_NUMBER() OVER (PARTITION BY r.name ORDER BY s.id) AS rn
FROM Sale s
JOIN Region r ON s.region = r.name
ORDER BY s.id
-- @expect:
-- | id: I64 | rn: I64 |
-- | ------- | ------- |
-- | 1       | 1       |
-- | 2       | 2       |
-- | 3       | 1       |
