CREATE TABLE GcdI64 (
    left INTEGER NULL DEFAULT GCD(3, 4),
    right INTEGER NULL DEFAULT LCM(10, 2)
)
-- expect: payload Create

INSERT INTO GcdI64 VALUES (0, 3), (2,4), (6,8), (3,5), (1, NULL), (NULL, 1);
-- expect: payload Insert
-- 6

SELECT GCD(left, right) AS test FROM GcdI64
-- expect:
-- | test: I64 |
-- | 3         |
-- | 2         |
-- | 2         |
-- | 1         |
-- | NULL      |
-- | NULL      |

CREATE TABLE GcdStr (
    left TEXT,
    right INTEGER
)
-- expect: payload Create

INSERT INTO GcdStr VALUES ('TEXT', 0);
-- expect: payload Insert
-- 1

SELECT GCD(left, right) AS test FROM GcdStr
-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "GCD"

SELECT GCD(right, left) AS test FROM GcdStr
-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "GCD"

CREATE TABLE LcmI64 (
    left INTEGER NULL DEFAULT true,
    right INTEGER NULL DEFAULT true
)
-- expect: payload Create

INSERT INTO LcmI64 VALUES (0, 3), (2,4), (6,8), (3,5), (1, NULL), (NULL, 1);
-- expect: payload Insert
-- 6

SELECT LCM(left, right) AS test FROM LcmI64
-- expect:
-- | test: I64 |
-- | 0         |
-- | 4         |
-- | 24        |
-- | 15        |
-- | NULL      |
-- | NULL      |

CREATE TABLE LcmStr (
    left TEXT,
    right INTEGER
)
-- expect: payload Create

INSERT INTO LcmStr VALUES ('TEXT', 0);
-- expect: payload Insert
-- 1

SELECT LCM(left, right) AS test FROM LcmStr
-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "LCM"

SELECT LCM(right, left) AS test FROM LcmStr
-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "LCM"

SELECT GCD(0, 0) as test
-- expect:
-- | test: I64 |
-- | 0         |

VALUES(
    GCD(-1, -1),
    GCD(-2, 0),
    GCD(-14, 7)
)
-- expect:
-- | column1: I64 | column2: I64 | column3: I64 |
-- | 1            | 2            | 7            |

SELECT GCD(-9223372036854775808, -9223372036854775808)
-- expect: error Evaluate.GcdLcmOverflow
-- -9223372036854775808

SELECT LCM(0, 0) as test
-- expect:
-- | test: I64 |
-- | 0         |

VALUES(
    LCM(-3, -5),
    LCM(-13, 0),
    LCM(-12, 2)
)
-- expect:
-- | column1: I64 | column2: I64 | column3: I64 |
-- | 15           | 0            | 12           |

SELECT LCM(-9223372036854775808, -9223372036854775808)
-- expect: error Evaluate.GcdLcmOverflow
-- -9223372036854775808

SELECT LCM(10000000019, 10000000033)
-- expect: error Evaluate.LcmResultOutOfRange

SELECT gcd(1.0, 1);
-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "GCD"
