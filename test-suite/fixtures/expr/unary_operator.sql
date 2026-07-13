CREATE TABLE Test (v1 INT, v2 FLOAT, v3 TEXT, v4 INT, v5 INT, v6 INT8)
-- expect: payload Create

INSERT INTO Test VALUES (10, 10.5, 'hello', -5, 1000, 20)
-- expect: payload Insert
-- 1

SELECT -v1 as v1, -v2 as v2, v3, -v4 as v4, -v6 as v6 FROM Test
-- expect:
-- | v1: I64 | v2: F64 | v3: Str | v4: I64 | v6: I8 |
-- | -10     | -10.5   | "hello" | 5       | -20    |

SELECT -(-10) as v1, -(-10) as v2 FROM Test
-- expect:
-- | v1: I64 | v2: I64 |
-- | 10      | 10      |

SELECT -v3 as v3 FROM Test
-- expect: error Value.UnaryMinusOnNonNumeric

SELECT -'errrr' as v1 FROM Test
-- expect: error Evaluate.UnsupportedUnaryMinus
-- "errrr"

SELECT +10 as v1, +(+10) as v2 FROM Test
-- expect:
-- | v1: I64 | v2: I64 |
-- | 10      | 10      |

SELECT +v3 as v3 FROM Test
-- expect: error Value.UnaryPlusOnNonNumeric

SELECT +'errrr' as v1 FROM Test
-- expect: error Evaluate.UnsupportedUnaryPlus
-- "errrr"

SELECT v1! as v1 FROM Test
-- expect:
-- | v1: I128 |
-- | 3628800  |

SELECT 4! as v1 FROM Test
-- expect:
-- | v1: I128 |
-- | 24       |

SELECT v2! as v1 FROM Test
-- expect: error Value.FactorialOnNonInteger

SELECT v3! as v1 FROM Test
-- expect: error Value.FactorialOnNonNumeric

SELECT v4! as v4 FROM Test
-- expect: error Value.FactorialOnNegativeNumeric

SELECT v5! as v5 FROM Test
-- expect: error Value.FactorialOverflow

SELECT (-v6)! as v6 FROM Test
-- expect: error Value.FactorialOnNegativeNumeric

SELECT (v6 * 2)! as v6 FROM Test
-- expect: error Value.FactorialOverflow

SELECT (-5)! as v4 FROM Test
-- expect: error Value.FactorialOnNegativeNumeric

SELECT (5.5)! as v4 FROM Test
-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int",
--   "literal": "5.5"
-- }

SELECT 'errrr'! as v1 FROM Test
-- expect: error Evaluate.UnaryFactorialRequiresNumericLiteral
-- "errrr"

SELECT 1000! as v4 FROM Test
-- expect: error Value.FactorialOverflow

-- name: test bitwise-not operator with UINT8 type
SELECT ~(CAST(1 AS UINT8)) as v1 FROM Test
-- expect:
-- | v1: U8 |
-- | 254    |

-- name: test bitwise-not operator with UINT16 type
SELECT ~(CAST(1 AS UINT16)) as v1 FROM Test
-- expect:
-- | v1: U16 |
-- | 65534   |

-- name: test bitwise-not operator with UINT32 type
SELECT ~(CAST(1 AS UINT32)) as v1 FROM Test
-- expect:
-- | v1: U32    |
-- | 4294967294 |

-- name: test bitwise-not operator with UINT64 type
SELECT ~(CAST(1 AS UINT64)) as v1 FROM Test
-- expect:
-- | v1: U64              |
-- | 18446744073709551614 |

-- name: test bitwise-not operator with UINT128 type
SELECT ~(CAST(1 AS UINT128)) as v1 FROM Test
-- expect:
-- | v1: U128                                |
-- | 340282366920938463463374607431768211454 |

-- name: test bitwise-not operator with INT8 type
SELECT ~(CAST(1 AS INT8)) as v1 FROM Test
-- expect:
-- | v1: I8 |
-- | -2     |

-- name: test bitwise-not operator with INT16 type
SELECT ~(CAST(1 AS INT16)) as v1 FROM Test
-- expect:
-- | v1: I16 |
-- | -2      |

-- name: test bitwise-not operator with INT32 type
SELECT ~(CAST(1 AS INT32)) as v1 FROM Test
-- expect:
-- | v1: I32 |
-- | -2      |

-- name: test bitwise-not operator with INT64 type
SELECT ~1 as v1 FROM Test
-- expect:
-- | v1: I64 |
-- | -2      |

-- name: test bitwise-not operator with INT128 type
SELECT ~(CAST(1 AS INT128)) as v1 FROM Test
-- expect:
-- | v1: I128 |
-- | -2       |

-- name: test bitwise-not operator with Null
SELECT ~Null as v1 FROM Test
-- expect:
-- | v1   |
-- | NULL |

-- name: test bitwise-not operator with FLOAT64 type
SELECT ~(5.5) as v4 FROM Test
-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int",
--   "literal": "5.5"
-- }

-- name: test bitwise-not operator with FLOAT32 type
SELECT ~(CAST(5.5 AS FLOAT32)) as v4 FROM Test
-- expect: error Value.UnaryBitwiseNotOnNonInteger

-- name: test bitwise-not operator with string type
SELECT ~'error' as v1 FROM Test
-- expect: error Evaluate.UnaryBitwiseNotRequiresIntegerLiteral
-- "error"
