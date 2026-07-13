CREATE TABLE line (x FLOAT32, y FLOAT32)
-- expect: payload Create

INSERT INTO line VALUES (0.3134, 0.156)
-- expect: payload Insert
-- 1

SELECT x, y FROM line;
-- expect:
-- | x: F32 | y: F32 |
-- | 0.3134 | 0.156  |

UPDATE line SET x=2.0, y=1.0 WHERE x=0.3134 AND y=0.156
-- expect: payload Update
-- 1

SELECT x, y FROM line
-- expect:
-- | x: F32 | y: F32 |
-- | 2.0    | 1.0    |

DELETE FROM line WHERE x=2.0 AND y=1.0
-- expect: payload Delete
-- 1

SELECT CAST('-71.064544' AS FLOAT32) AS float32
-- expect:
-- | float32: F32 |
-- | -71.064545   |
