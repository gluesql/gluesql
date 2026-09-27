CREATE TABLE LongArrowSample (object MAP, array LIST);
-- @expect: ok

INSERT INTO LongArrowSample VALUES (
    '{"id":1,"b":2,"name":"Han","price":4.25,"active":true,"nested":{"role":"admin"},"1":"first"}',
    '[1,"two",true,4.25,null]'
);
-- @expect: ok

SELECT object->>'id' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "1"         |

SELECT object->>'b' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "2"         |

SELECT object->>'name' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "Han"       |

SELECT object->>'price' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "4.25"      |

SELECT object->>'active' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "TRUE"      |

SELECT object->>'nested' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str            |
-- | ---------------------- |
-- | "{\"role\":\"admin\"}" |

SELECT object->>1 AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "first"     |

SELECT object->>CAST(1 AS INT16) AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "first"     |

SELECT object->>'missing' AS result FROM LongArrowSample;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT object->>NULL AS result FROM LongArrowSample;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT array->>0 AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "1"         |

SELECT array->>1 AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "two"       |

SELECT array->>2 AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "TRUE"      |

SELECT array->>3 AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "4.25"      |

SELECT array->>'3' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "4.25"      |

SELECT array->>4 AS result FROM LongArrowSample;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT array->>(-1) AS result FROM LongArrowSample;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT array->>100 AS result FROM LongArrowSample;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT array->>'foo' AS result FROM LongArrowSample;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT array->>-1 AS result FROM LongArrowSample;
-- @expect: error Translate.UnsupportedBinaryOperator
-- @json: "->>-"

SELECT array->>CAST(-1 AS INT16) AS result FROM LongArrowSample;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT NULL->>'role' AS result;
-- @expect:
-- | result |
-- | ------ |
-- | NULL   |

SELECT object->'nested'->>'role' AS result FROM LongArrowSample;
-- @expect:
-- | result: Str |
-- | ----------- |
-- | "admin"     |

SELECT 1 ->> 'foo' AS result;
-- @expect: error Evaluate.ArrowBaseRequiresMapOrList

SELECT TRUE ->> 'foo' AS result;
-- @expect: error Evaluate.ArrowBaseRequiresMapOrList

SELECT '{"role":"admin"}' ->> 'role' AS result;
-- @expect: error Evaluate.ArrowBaseRequiresMapOrList

SELECT object->>TRUE AS result FROM LongArrowSample;
-- @expect: error Evaluate.ArrowSelectorRequiresIntegerOrString
-- @json: "Bool(true)"
