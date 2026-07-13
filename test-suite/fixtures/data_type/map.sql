CREATE TABLE MapType (
    id INTEGER NULL DEFAULT UNWRAP(NULL, 'a'),
    nested MAP
)

-- expect: ok

INSERT INTO MapType VALUES
    (1, '{"a": true, "b": 2}'),
    (2, '{"a": {"foo": "ok", "b": "steak"}, "b": 30}'),
    (3, '{"a": {"b": {"c": {"d": 10}}}}');

-- expect: ok

SELECT id, nested FROM MapType LIMIT 1

-- expect:
-- | id: I64 | nested: Map      |
-- | 1       | {"a":true,"b":2} |

SELECT
        id,
        UNWRAP(nested, 'a.foo') || '.yeah' AS foo,
        UNWRAP(nested, 'a.b.c.d') as good,
        UNWRAP(nested, 'a.b.c.d') * 2 as good2,
        UNWRAP(nested, 'a.b') as b
    FROM MapType

-- expect:
-- | id: I64 | foo: Str  | good: I64 | good2: I64 | b                   |
-- | 1       | NULL      | NULL      | NULL       | NULL                |
-- | 2       | "ok.yeah" | NULL      | NULL       | Str("steak")        |
-- | 3       | NULL      | 10        | 20         | Map({"c":{"d":10}}) |

SELECT
        id,
        UNWRAP(NULL, 'a.b') as foo,
        UNWRAP(nested, NULL) as bar
    FROM MapType LIMIT 1

-- expect:
-- | id: I64 | foo  | bar  |
-- | 1       | NULL | NULL |

CREATE TABLE MapType2 (
    id INTEGER,
    nested MAP
)

-- expect: ok

INSERT INTO MapType2 VALUES
    (1, '{"a": {"red": "apple", "blue": 1}, "b": 10}'),
    (2, '{"a": {"red": "cherry", "blue": 2}, "b": 20}'),
    (3, '{"a": {"red": "berry", "blue": 3}, "b": 30, "c": true}');

-- expect: ok

SELECT id, nested['b'] as b FROM MapType2

-- expect:
-- | id: I64 | b: I64 |
-- | 1       | 10     |
-- | 2       | 20     |
-- | 3       | 30     |

-- name: select index expr without alias
SELECT id, nested['b'] FROM MapType2

-- expect:
-- | id: I64 | nested['b']: I64 |
-- | 1       | 10               |
-- | 2       | 20               |
-- | 3       | 30               |

-- name: index expr with non-existent key from MapType Value returns Null
SELECT
        id,
        nested['a']['red'] AS fruit,
        nested['a']['blue'] + nested['b'] as sum,
        nested['c'] AS c
    FROM MapType2

-- expect:
-- | id: I64 | fruit: Str | sum: I64 | c: Bool |
-- | 1       | "apple"    | 11       | NULL    |
-- | 2       | "cherry"   | 22       | NULL    |
-- | 3       | "berry"    | 33       | true    |

-- name: cast literal to MAP
SELECT CAST('{"a": 1}' AS MAP) AS map

-- expect:
-- | map: Map |
-- | {"a":1}  |

SELECT UNWRAP('abc', 'a.b.c') FROM MapType

-- expect: error Evaluate.FunctionRequiresMapValue
-- "UNWRAP"

SELECT UNWRAP(id, 'a.b.c') FROM MapType

-- expect: error Value.SelectorRequiresMapOrListTypes

SELECT id, nested['a']['blue']['first'] FROM MapType2

-- expect: error Value.SelectorRequiresMapOrListTypes

SELECT id FROM MapType GROUP BY nested

-- expect:
-- | id: I64 |
-- | 1       |
-- | 2       |
-- | 3       |

INSERT INTO MapType VALUES (1, '{{ ok [1, 2, 3] }');

-- expect: error Value.InvalidJsonString
-- "{{ ok [1, 2, 3] }"

INSERT INTO MapType VALUES (1, '[1, 2, 3]');

-- expect: error Value.JsonObjectTypeRequired
