CREATE TABLE ArrowSample (object MAP, array LIST);

-- expect: ok

INSERT INTO ArrowSample VALUES (
    '{"id":1,"b":2,"name":"Han","price":4.25,"active":true,"nested":{"role":"admin"},"1":"first"}',
    '[1,"two",true,4.25,null]'
);

-- expect: ok

SELECT object->'b' AS result FROM ArrowSample;

-- expect:
-- | result: I64 |
-- | 2           |

SELECT object->'name' AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "Han"       |

SELECT object->'price' AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

SELECT object->'active' AS result FROM ArrowSample;

-- expect:
-- | result: Bool |
-- | true         |

SELECT object->'nested' AS result FROM ArrowSample;

-- expect:
-- | result: Map      |
-- | {"role":"admin"} |

SELECT object->1 AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

SELECT object->CAST(1 AS INT16) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

SELECT object->'missing' AS result FROM ArrowSample;

-- expect:
-- | result |
-- | NULL   |

SELECT object->NULL AS result FROM ArrowSample;

-- expect:
-- | result |
-- | NULL   |

SELECT array->0 AS result FROM ArrowSample;

-- expect:
-- | result: I64 |
-- | 1           |

SELECT array->1 AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "two"       |

SELECT array->2 AS result FROM ArrowSample;

-- expect:
-- | result: Bool |
-- | true         |

SELECT array->3 AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

SELECT array->4 AS result FROM ArrowSample;

-- expect:
-- | result |
-- | NULL   |

SELECT array->'3' AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

SELECT array->'foo' AS result FROM ArrowSample;

-- expect:
-- | result |
-- | NULL   |

SELECT array->-1 AS result FROM ArrowSample;

-- expect: error Translate.UnsupportedBinaryOperator
-- "->-"

SELECT array->(-1) AS result FROM ArrowSample;

-- expect:
-- | result |
-- | NULL   |

SELECT array->CAST(-1 AS INT16) AS result FROM ArrowSample;

-- expect:
-- | result |
-- | NULL   |

SELECT 1 -> 'foo' AS result;

-- expect: error Evaluate.ArrowBaseRequiresMapOrList

SELECT TRUE -> 'foo' AS result;

-- expect: error Evaluate.ArrowBaseRequiresMapOrList

SELECT '{"role":"admin"}'->'role' AS result;

-- expect: error Evaluate.ArrowBaseRequiresMapOrList

SELECT object->TRUE AS result FROM ArrowSample;

-- expect: error Evaluate.ArrowSelectorRequiresIntegerOrString
-- "Bool(true)"

SELECT NULL->'role' AS result;

-- expect:
-- | result |
-- | NULL   |

-- name: Arrow map selector uses INT8
SELECT object->CAST(1 AS INT8) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses INT16
SELECT object->CAST(1 AS INT16) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses INT32
SELECT object->CAST(1 AS INT32) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses INT64
SELECT object->CAST(1 AS INT64) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses INT128
SELECT object->CAST(1 AS INT128) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses UINT8
SELECT object->CAST(1 AS UINT8) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses UINT16
SELECT object->CAST(1 AS UINT16) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses UINT32
SELECT object->CAST(1 AS UINT32) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses UINT64
SELECT object->CAST(1 AS UINT64) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow map selector uses UINT128
SELECT object->CAST(1 AS UINT128) AS result FROM ArrowSample;

-- expect:
-- | result: Str |
-- | "first"     |

-- name: Arrow selector uses INT8
SELECT array->CAST(3 AS INT8) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses INT16
SELECT array->CAST(3 AS INT16) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses INT32
SELECT array->CAST(3 AS INT32) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses INT64
SELECT array->CAST(3 AS INT64) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses INT128
SELECT array->CAST(3 AS INT128) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses UINT8
SELECT array->CAST(3 AS UINT8) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses UINT16
SELECT array->CAST(3 AS UINT16) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses UINT32
SELECT array->CAST(3 AS UINT32) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses UINT64
SELECT array->CAST(3 AS UINT64) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |

-- name: Arrow selector uses UINT128
SELECT array->CAST(3 AS UINT128) AS result FROM ArrowSample;

-- expect:
-- | result: F64 |
-- | 4.25        |
