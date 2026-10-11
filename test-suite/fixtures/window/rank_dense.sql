CREATE TABLE Score (
    id INTEGER,
    points INTEGER
);
-- @expect: ok

INSERT INTO Score (id, points) VALUES
    (1, 300),
    (2, 300),
    (3, 200),
    (4, 100),
    (5, 100),
    (6, 50);
-- @expect: ok

SELECT
    id,
    RANK() OVER (ORDER BY points DESC) AS r,
    DENSE_RANK() OVER (ORDER BY points DESC) AS d
FROM Score ORDER BY id
-- @expect:
-- | id: I64 | r: I64 | d: I64 |
-- | ------- | ------ | ------ |
-- | 1       | 1      | 1      |
-- | 2       | 1      | 1      |
-- | 3       | 3      | 2      |
-- | 4       | 4      | 3      |
-- | 5       | 4      | 3      |
-- | 6       | 6      | 4      |

SELECT
    id,
    RANK() OVER (PARTITION BY points ORDER BY id) AS r
FROM Score WHERE points = 100 ORDER BY id
-- @expect:
-- | id: I64 | r: I64 |
-- | ------- | ------ |
-- | 4       | 1      |
-- | 5       | 2      |
