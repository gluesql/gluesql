CREATE TABLE USER (id INTEGER, data MAP);
-- @expect: ok

INSERT INTO USER VALUES
    (1, '{"id": 1, "name": "alice", "is_male": false}'),
    (2, '{"name": "bob"}'),
    (3, '{}');
-- @expect: ok

-- @name: return all values from map by descending order
SELECT SORT(VALUES(data), 'DESC') as result FROM USER WHERE id=1
-- @expect:
-- | result: List      |
-- | [1,false,"alice"] |

-- @name: return all values from map by ascending order
SELECT SORT(VALUES(data), 'ASC') as result FROM USER WHERE id=1
-- @expect:
-- | result: List      |
-- | ["alice",false,1] |

-- @name: return all values from map
SELECT VALUES(data) as result FROM USER WHERE id=2
-- @expect:
-- | result: List |
-- | ["bob"]      |

-- @name: return null from empty map
SELECT VALUES(data) as result FROM USER WHERE id=3
-- @expect:
-- | result: List |
-- | []           |

-- @name: return argument type error
SELECT VALUES(id) FROM USER WHERE id=1
-- @expect: error Evaluate.MapTypeRequired
