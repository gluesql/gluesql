CREATE TABLE Reading (
    id INTEGER,
    seq INTEGER,
    v INTEGER
);
-- @expect: ok

INSERT INTO Reading (id, seq, v) VALUES
    (1, 1, 10),
    (2, 2, 20),
    (3, 2, 30),
    (4, 3, 40);
-- @expect: ok

SELECT
    id,
    SUM(v) OVER (ORDER BY seq) AS s,
    COUNT(*) OVER (ORDER BY seq) AS c,
    AVG(v) OVER (ORDER BY seq) AS a,
    MAX(v) OVER (ORDER BY seq) AS m
FROM Reading ORDER BY id
-- @expect:
-- | id: I64 | s: I64 | c: I64 | a: F64 | m: I64 |
-- | ------- | ------ | ------ | ------ | ------ |
-- | 1       | 10     | 1      | 10.0   | 10     |
-- | 2       | 60     | 3      | 20.0   | 30     |
-- | 3       | 60     | 3      | 20.0   | 30     |
-- | 4       | 100    | 4      | 25.0   | 40     |

CREATE TABLE Ledger (
    id INTEGER,
    region TEXT,
    seq INTEGER,
    v INTEGER
);
-- @expect: ok

INSERT INTO Ledger (id, region, seq, v) VALUES
    (1, 'East', 1, 10),
    (2, 'East', 2, 20),
    (3, 'West', 1, 5),
    (4, 'West', 2, 15);
-- @expect: ok

SELECT id, SUM(v) OVER (PARTITION BY region ORDER BY seq) AS s FROM Ledger ORDER BY id
-- @expect:
-- | id: I64 | s: I64 |
-- | ------- | ------ |
-- | 1       | 10     |
-- | 2       | 30     |
-- | 3       | 5      |
-- | 4       | 20     |

CREATE TABLE Gap (
    id INTEGER,
    v INTEGER NULL
);
-- @expect: ok

INSERT INTO Gap (id, v) VALUES
    (1, NULL),
    (2, 10),
    (3, NULL),
    (4, 5);
-- @expect: ok

-- Aggregates ignore NULL inputs, so a leading NULL must not poison the running value.
SELECT
    id,
    MIN(v) OVER (ORDER BY id) AS mn,
    MAX(v) OVER (ORDER BY id) AS mx,
    SUM(v) OVER (ORDER BY id) AS s,
    AVG(v) OVER (ORDER BY id) AS a,
    COUNT(v) OVER (ORDER BY id) AS c
FROM Gap ORDER BY id
-- @expect:
-- | id: I64 | mn      | mx      | s       | a         | c: I64 |
-- | ------- | ------- | ------- | ------- | --------- | ------ |
-- | 1       | NULL    | NULL    | NULL    | NULL      | 0      |
-- | 2       | I64(10) | I64(10) | I64(10) | F64(10.0) | 1      |
-- | 3       | I64(10) | I64(10) | I64(10) | F64(10.0) | 1      |
-- | 4       | I64(5)  | I64(10) | I64(15) | F64(7.5)  | 2      |
