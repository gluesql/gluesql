CREATE TABLE Test (
    id INTEGER,
    lhs INTEGER,
    rhs INTEGER
);

-- expect: ok

INSERT INTO Test
    VALUES
        (1, 29, 15);

-- expect: ok

-- name: bitwise-and for values
SELECT lhs & rhs AS and_result FROM Test

-- expect:
-- | and_result: I64 |
-- | 13              |

-- name: bitwise-and for literals
SELECT 29 & 15 AS column1;

-- expect:
-- | column1: I64 |
-- | 13           |

-- name: bitwise-and between a value and a literal
SELECT 29 & rhs AS and_result FROM Test

-- expect:
-- | and_result: I64 |
-- | 13              |

-- name: bitwise_and between multiple values
SELECT 29 & rhs & 3 AS and_result FROM Test

-- expect:
-- | and_result: I64 |
-- | 1               |

-- name: bitwise_and between wrong type values shoud occurs error
SELECT 1.1 & 12 AS and_result FROM Test

-- expect: error Evaluate.IncompatibleBitOperation
-- [
--   "1.1",
--   "12"
-- ]

-- name: bitwise_and between null and value
SELECT null & rhs AS and_result from Test

-- expect:
-- | and_result |
-- | NULL       |

-- name: bitwise_and between value and null
SELECT rhs & null AS and_result from Test

-- expect:
-- | and_result |
-- | NULL       |

-- name: bitwise_and between null and literal
SELECT null & 12 AS and_result from Test

-- expect:
-- | and_result |
-- | NULL       |

-- name: bitwise_and between literal and null
SELECT 12 & null AS and_result from Test

-- expect:
-- | and_result |
-- | NULL       |

-- name: bitwise_and for unsupported value
SELECT 'ss' & 'sp' AS and_result from Test

-- expect: error Evaluate.UnsupportedBinaryOperation
-- {
--   "left": "ss",
--   "op": "BitwiseAnd",
--   "right": "sp"
-- }
