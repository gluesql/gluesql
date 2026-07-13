CREATE TABLE ListType (
    id INTEGER,
    items LIST
)
-- expect: ok

INSERT INTO ListType VALUES
    (1, '[1, 2, 3]'),
    (2, '["hello", "world", 30, true, [9,8]]'),
    (3, '[{ "foo": 100, "bar": [true, 0, [10.5, false] ] }, 10, 20]');
-- expect: ok

SELECT id, items FROM ListType
-- expect:
-- | id: I64 | items: List                                     |
-- | 1       | [1,2,3]                                         |
-- | 2       | ["hello","world",30,true,[9,8]]                 |
-- | 3       | [{"bar":[true,0,[10.5,false]],"foo":100},10,20] |

SELECT
    id,
    UNWRAP(items, '1') AS foo,
    UNWRAP(items, '0.foo') + 100 AS bar,
    UNWRAP(items, '4') AS a,
    UNWRAP(items, '0.bar.2.0') + UNWRAP(items, '2') AS b
FROM ListType
-- expect:
-- | id: I64 | foo          | bar: I64 | a: List | b: F64 |
-- | 1       | I64(2)       | NULL     | NULL    | NULL   |
-- | 2       | Str("world") | NULL     | [9,8]   | NULL   |
-- | 3       | I64(10)      | 200      | NULL    | 30.5   |

SELECT id, items[1] AS second FROM ListType
-- expect:
-- | id: I64 | second       |
-- | 1       | I64(2)       |
-- | 2       | Str("world") |
-- | 3       | I64(10)      |

-- name: select index expr without alias
SELECT id, items[1] FROM ListType
-- expect:
-- | id: I64 | items[1]     |
-- | 1       | I64(2)       |
-- | 2       | Str("world") |
-- | 3       | I64(10)      |

CREATE TABLE ListType2 (
    id INTEGER,
    items LIST
)
-- expect: ok

INSERT INTO ListType2 VALUES
    (1, '[1, 2, 3, { "hi": "bye" }]'),
    (2, '["one", "two", "three", [100, 200]]'),
    (3, '["first", "second", "third", { "foo": true, "bar": false }]');
-- expect: ok

SELECT
    id,
    items['0'] AS foo,
    items['1'] AS bar,
    items['3']['0'] AS hundred
FROM ListType2
-- expect:
-- | id: I64 | foo          | bar           | hundred: I64 |
-- | 1       | I64(1)       | I64(2)        | NULL         |
-- | 2       | Str("one")   | Str("two")    | 100          |
-- | 3       | Str("first") | Str("second") | NULL         |

-- name: cast literal to LIST
SELECT CAST('[1, 2, 3]' AS LIST) AS list
-- expect:
-- | list: List |
-- | [1,2,3]    |

SELECT id, items['not']['list'] AS foo FROM ListType2
-- expect: error Value.SelectorRequiresMapOrListTypes

SELECT id FROM ListType GROUP BY items
-- expect:
-- | id: I64 |
-- | 1       |
-- | 2       |
-- | 3       |

INSERT INTO ListType VALUES (1, '{ "a": 10 }');
-- expect: error Value.JsonArrayTypeRequired

INSERT INTO ListType VALUES (1, '{{ ok [1, 2, 3] }');
-- expect: error Value.InvalidJsonString
-- "{{ ok [1, 2, 3] }"
