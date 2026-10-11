CREATE TABLE Entry (
    id INTEGER,
    tag TEXT NULL,
    bucket INTEGER,
    v INTEGER
);
-- @expect: ok

INSERT INTO Entry (id, tag, bucket, v) VALUES
    (1, 'x',  1, 10),
    (2, 'x',  1, 20),
    (3, 'x',  2, 30),
    (4, NULL, 1, 40),
    (5, NULL, 1, 50),
    (6, 'y',  1, 60);
-- @expect: ok

SELECT id, COUNT(*) OVER (PARTITION BY tag, bucket) AS c FROM Entry ORDER BY id
-- @expect:
-- | id: I64 | c: I64 |
-- | ------- | ------ |
-- | 1       | 2      |
-- | 2       | 2      |
-- | 3       | 1      |
-- | 4       | 2      |
-- | 5       | 2      |
-- | 6       | 1      |

SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM Entry WHERE v >= 30 ORDER BY id
-- @expect:
-- | id: I64 | rn: I64 |
-- | ------- | ------- |
-- | 3       | 1       |
-- | 4       | 2       |
-- | 5       | 3       |
-- | 6       | 4       |

SELECT id, SUM(v) OVER (PARTITION BY tag ORDER BY id) AS s FROM Entry ORDER BY id
-- @expect:
-- | id: I64 | s: I64 |
-- | ------- | ------ |
-- | 1       | 10     |
-- | 2       | 30     |
-- | 3       | 60     |
-- | 4       | 40     |
-- | 5       | 90     |
-- | 6       | 60     |
