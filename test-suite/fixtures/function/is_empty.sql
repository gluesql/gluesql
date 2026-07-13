CREATE TABLE IsEmpty (
    id INTEGER,
    list_items LIST,
    map_items MAP
);
-- expect: ok

INSERT INTO IsEmpty VALUES
    (1, '[]', '{"a": {"red": "cherry", "blue": 2}, "b": 20}'),
    (2, '[1, 2, 3]', '{"a": {"red": "berry", "blue": 3}, "b": 30, "c": true}'),
    (3, '[]', '{}'),
    (4, '[10]', '{}');
-- expect: ok

-- name: is_empty for list, return true
SELECT id FROM IsEmpty WHERE IS_EMPTY(list_items);
-- expect:
-- | id: I64 |
-- | 1       |
-- | 3       |

-- name: is_empty for list, return false
SELECT IS_EMPTY(list_items) as result FROM IsEmpty WHERE id=2;
-- expect:
-- | result: Bool |
-- | false        |

-- name: is_empty for map, return true
SELECT id FROM IsEmpty WHERE IS_EMPTY(map_items);
-- expect:
-- | id: I64 |
-- | 3       |
-- | 4       |

-- name: is_empty for map, return false
SELECT IS_EMPTY(map_items) as result FROM IsEmpty WHERE id=1;
-- expect:
-- | result: Bool |
-- | false        |

-- name: other argument types, return error
SELECT id FROM IsEmpty WHERE IS_EMPTY(id);
-- expect: error Evaluate.MapOrListTypeRequired
