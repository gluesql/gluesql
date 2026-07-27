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
