CREATE TABLE Arith (
    id INTEGER,
    num INTEGER
);
-- @expect: ok

DELETE FROM Arith
-- @expect: ok

INSERT INTO Arith (id, num) VALUES
    (1, 6),
    (2, 8),
    (3, 4),
    (4, 2),
    (5, 3);
-- @expect: ok

SELECT 1 * 2 + 1 - 3 / 1 FROM Arith LIMIT 1;
-- @expect:
-- | 1 * 2 + 1 - 3 / 1: I64 |
-- | ---------------------- |
-- | 0                      |

SELECT id, id + 1, id + num, 1 + 1 FROM Arith
-- @expect:
-- | id: I64 | id + 1: I64 | id + num: I64 | 1 + 1: I64 |
-- | ------- | ----------- | ------------- | ---------- |
-- | 1       | 2           | 7             | 2          |
-- | 2       | 3           | 10            | 2          |
-- | 3       | 4           | 7             | 2          |
-- | 4       | 5           | 6             | 2          |
-- | 5       | 6           | 8             | 2          |

SELECT a.id + b.id FROM Arith a JOIN Arith b ON a.id = b.id + 1
-- @expect:
-- | a.id + b.id: I64 |
-- | ---------------- |
-- | 3                |
-- | 5                |
-- | 7                |
-- | 9                |

SELECT TRUE XOR TRUE, FALSE XOR FALSE, TRUE XOR FALSE, FALSE XOR TRUE FROM Arith LIMIT 1
-- @expect:
-- | true XOR true: Bool | false XOR false: Bool | true XOR false: Bool | false XOR true: Bool |
-- | ------------------- | --------------------- | -------------------- | -------------------- |
-- | false               | false                 | true                 | true                 |
