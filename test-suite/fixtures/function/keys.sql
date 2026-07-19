CREATE TABLE USER (id INTEGER, data MAP);
-- @expect: ok

INSERT INTO USER VALUES
    (1, '{"id": 1, "name": "alice", "is_male": false}'),
    (2, '{"name": "bob"}'),
    (3, '{}');
-- @expect: ok

-- @name: return all keys from map by ascending order
SELECT SORT(KEYS(data), 'ASC') as result FROM USER WHERE id=1
-- @expect:
-- | result: List            |
-- | ["id","is_male","name"] |

-- @name: return one key from map
SELECT KEYS(data) as result FROM USER WHERE id=2
-- @expect:
-- | result: List |
-- | ["name"]     |

-- @name: return null from empty map
SELECT KEYS(data) as result FROM USER WHERE id=3
-- @expect:
-- | result: List |
-- | []           |

-- @name: return argument type error
SELECT KEYS(id) FROM USER WHERE id=1
-- @expect: error Evaluate.MapTypeRequired
