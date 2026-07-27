CREATE TABLE M (
    id INTEGER,
    grp TEXT,
    a INTEGER,
    c INTEGER NULL
);
-- @expect: ok

INSERT INTO M (id, grp, a, c) VALUES
    (1, 'x', 1, 30),
    (2, 'x', 1, NULL),
    (3, 'x', 2, 10),
    (4, 'y', 1, 40);
-- @expect: ok

SELECT
    id,
    RANK() OVER (PARTITION BY grp) AS r,
    DENSE_RANK() OVER (PARTITION BY grp) AS d
FROM M ORDER BY id
-- @expect:
-- | id: I64 | r: I64 | d: I64 |
-- | ------- | ------ | ------ |
-- | 1       | 1      | 1      |
-- | 2       | 1      | 1      |
-- | 3       | 1      | 1      |
-- | 4       | 1      | 1      |

SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY a ASC, id DESC) AS rn FROM M ORDER BY id
-- @expect:
-- | id: I64 | rn: I64 |
-- | ------- | ------- |
-- | 1       | 2       |
-- | 2       | 1       |
-- | 3       | 3       |
-- | 4       | 1       |

SELECT id, ROW_NUMBER() OVER (ORDER BY c) AS rn FROM M ORDER BY id
-- @expect:
-- | id: I64 | rn: I64 |
-- | ------- | ------- |
-- | 1       | 2       |
-- | 2       | 4       |
-- | 3       | 1       |
-- | 4       | 3       |

SELECT id, MIN(c) OVER (ORDER BY id) AS m FROM M ORDER BY id
-- @expect:
-- | id: I64 | m: I64 |
-- | ------- | ------ |
-- | 1       | 30     |
-- | 2       | 30     |
-- | 3       | 10     |
-- | 4       | 10     |

SELECT
    id,
    LAG(a, 2) OVER (ORDER BY id) AS lg2,
    LEAD(a, 0, 99) OVER (ORDER BY id) AS ld0
FROM M ORDER BY id
-- @expect:
-- | id: I64 | lg2    | ld0: I64 |
-- | ------- | ------ | -------- |
-- | 1       | NULL   | 1        |
-- | 2       | NULL   | 1        |
-- | 3       | I64(1) | 2        |
-- | 4       | I64(1) | 1        |
