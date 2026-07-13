CREATE TABLE DECIMAL_ITEM (v DECIMAL)
-- expect: ok

INSERT INTO DECIMAL_ITEM VALUES (1)
-- expect: ok

SELECT
        v AS a,
        v + 1 AS b,
        1 + v AS c,
        v - 1 AS d,
        1 - v AS e,
        v * 2 AS f,
        2 * v AS g
    FROM DECIMAL_ITEM
-- expect:
-- | a: Decimal | b: Decimal | c: I64 | d: Decimal | e: I64 | f: Decimal | g: I64 |
-- | 1          | 2          | 2      | 0          | 0      | 2          | 2      |

SELECT
        v / 2 AS h,
        2 / v AS i,
        2 % v AS j,
        v % 2 AS k
    FROM DECIMAL_ITEM
-- expect:
-- | h: Decimal | i: I64 | j: I64 | k: Decimal |
-- | 0.5        | 2      | 0      | 1          |

INSERT INTO DECIMAL_ITEM VALUES (1.5), (2.0), (25.12)
-- expect: ok

SELECT v FROM DECIMAL_ITEM WHERE v > 1.5 AND v <= 25.12
-- expect:
-- | v: Decimal |
-- | 2          |
-- | 25.12      |
