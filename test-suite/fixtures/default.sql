CREATE TABLE Test (
    id INTEGER DEFAULT 1,
    num INTEGER,
    flag BOOLEAN NULL DEFAULT false
)
-- expect: payload Create

INSERT INTO Test VALUES (8, 80, true);
-- expect: payload Insert
-- 1

INSERT INTO Test (num) VALUES (10);
-- expect: payload Insert
-- 1

INSERT INTO Test (num, id) VALUES (20, 2);
-- expect: payload Insert
-- 1

INSERT INTO Test (num, flag) VALUES (30, NULL), (40, true);
-- expect: payload Insert
-- 2

SELECT * FROM Test;
-- expect:
-- | id: I64 | num: I64 | flag: Bool |
-- | 8       | 80       | true       |
-- | 1       | 10       | false      |
-- | 2       | 20       | false      |
-- | 1       | 30       | NULL       |
-- | 1       | 40       | true       |

CREATE TABLE FunctionTest (
    uuid UUID,
    num FLOAT
)
-- expect: payload Create

INSERT INTO FunctionTest VALUES (GENERATE_UUID(), 1.0)
-- expect: payload Insert
-- 1

INSERT INTO FunctionTest VALUES (GENERATE_UUID(), (SELECT id FROM Foo))
-- expect: error Evaluate.SubqueryNotAllowedInStatelessExpr

CREATE TABLE TestExpr (
    id INTEGER,
    date DATE DEFAULT DATE '2020-01-01',
    num INTEGER DEFAULT -(-1 * +2),
    flag BOOLEAN DEFAULT CAST('TRUE' AS BOOLEAN),
    flag2 BOOLEAN DEFAULT 1 IN (1, 2, 3),
    flag3 BOOLEAN DEFAULT 10 BETWEEN 1 AND 2,
    flag4 BOOLEAN DEFAULT (1 IS NULL OR NULL IS NOT NULL)
)
-- expect: payload Create

INSERT INTO TestExpr (id) VALUES (1);
-- expect: ok

SELECT * FROM TestExpr
-- expect:
-- | id: I64 | date: Date   | num: I64 | flag: Bool | flag2: Bool | flag3: Bool | flag4: Bool |
-- | 1       | "2020-01-01" | 2        | true       | true        | false       | false       |
