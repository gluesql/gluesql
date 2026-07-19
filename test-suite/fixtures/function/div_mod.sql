CREATE TABLE FloatDiv (
    dividend FLOAT DEFAULT MOD(30, 11),
    divisor FLOAT DEFAULT DIV(3, 2)
)
-- @expect: payload Create

INSERT INTO
    FloatDiv (dividend, divisor)
VALUES
    (12.5, 2.5), (12.34, 56.78), (-12.3, 4.0)
-- @expect: payload Insert
-- @json: 3

SELECT
    DIV(dividend, divisor),
    MOD(dividend, divisor)
FROM FloatDiv
-- @expect:
-- | DIV(dividend, divisor): I64 | MOD(dividend, divisor): F64 |
-- | --------------------------- | --------------------------- |
-- | 5                           | 0.0                         |
-- | 0                           | 12.34                       |
-- | -3                          | -0.3                        |

SELECT DIV(1.0, 0.0) AS quotient FROM FloatDiv
-- @expect: error Evaluate.DivisorShouldNotBeZero

SELECT DIV(1.0, 'dividend') AS quotient FROM FloatDiv
-- @expect: error Evaluate.FunctionRequiresFloatOrIntegerValue
-- @json: "DIV"

SELECT DIV(1.0) AS quotient FROM FloatDiv
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 2,
--   "found": 1,
--   "name": "DIV"
-- }

SELECT MOD(1.0, 2, 3) AS remainder FROM FloatDiv
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 2,
--   "found": 3,
--   "name": "MOD"
-- }

CREATE TABLE IntDiv (dividend INTEGER, divisor INTEGER)
-- @expect: payload Create

INSERT INTO
    IntDiv (dividend, divisor)
VALUES
    (12, 3), (12, 7), (12, 34), (-12, 7)
-- @expect: payload Insert
-- @json: 4

INSERT INTO IntDiv (dividend, divisor) VALUES (12, 2)
-- @expect: payload Insert
-- @json: 1

SELECT
    DIV(dividend, divisor),
    MOD(dividend, divisor)
FROM IntDiv
-- @expect:
-- | DIV(dividend, divisor): I64 | MOD(dividend, divisor): I64 |
-- | --------------------------- | --------------------------- |
-- | 4                           | 0                           |
-- | 1                           | 5                           |
-- | 0                           | 12                          |
-- | -1                          | -5                          |
-- | 6                           | 0                           |

SELECT DIV(1, 0) AS quotient FROM IntDiv
-- @expect: error Evaluate.DivisorShouldNotBeZero

CREATE TABLE MixDiv (dividend INTEGER NULL, divisor FLOAT NULL)
-- @expect: payload Create

INSERT INTO
    MixDiv (dividend, divisor)
VALUES
    (12, 3.0), (12, 34.0),
    (12, NULL), (NULL, 34.0), (NULL, NULL)
-- @expect: payload Insert
-- @json: 5

SELECT
    DIV(dividend, divisor),
    MOD(dividend, divisor)
FROM MixDiv
-- @expect:
-- | DIV(dividend, divisor): I64 | MOD(dividend, divisor): I64 |
-- | --------------------------- | --------------------------- |
-- | 4                           | 0                           |
-- | 0                           | 12                          |
-- | NULL                        | NULL                        |
-- | NULL                        | NULL                        |
-- | NULL                        | NULL                        |
