CREATE TABLE Bytea (bytes BYTEA)
-- expect: payload Create

INSERT INTO Bytea VALUES
    (X'123456'),
    ('ab0123'),
    (X'936DA0');
-- expect: payload Insert
-- 3

SELECT * FROM Bytea
-- expect:
-- | bytes: Bytea |
-- | "123456"     |
-- | "ab0123"     |
-- | "936da0"     |

INSERT INTO Bytea VALUES (0)
-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Bytea",
--   "literal": "0"
-- }

INSERT INTO Bytea VALUES (X'123')
-- expect: error Translate.FailedToDecodeHexString
-- "123"
