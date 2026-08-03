CREATE TABLE Player
-- @expect: ok

INSERT INTO Player VALUES ('{"flag":1,"id":1001,"name":"Beam"}'), ('{"id":1002,"name":"Seo"}');
-- @expect: ok

CREATE TABLE Item
-- @expect: ok

INSERT INTO Item VALUES ('{"dex":324,"id":100,"name":"Test 001","obj":{"cost":3000},"rare":false}'), ('{"id":200}');
-- @expect: ok

SELECT name, dex, rare FROM Item WHERE id = 100
-- @expect:
-- | name: Str  | dex: I64 | rare: Bool |
-- | ---------- | -------- | ---------- |
-- | "Test 001" | 324      | false      |

SELECT name, dex, rare FROM Item
-- @expect:
-- | name: Str  | dex: I64 | rare: Bool |
-- | ---------- | -------- | ---------- |
-- | "Test 001" | 324      | false      |
-- | NULL       | NULL     | NULL       |

SELECT * FROM Item
-- @expect: maps
-- | {"dex":324,"id":100,"name":"Test 001","obj":{"cost":3000},"rare":false} |
-- | {"id":200}                                                              |

-- @name: INSERT SELECT preserves schemaless map rows
CREATE TABLE MapCopy
-- @expect: ok

INSERT INTO MapCopy SELECT * FROM Item
-- @expect: ok

SELECT * FROM MapCopy
-- @expect: maps
-- | {"dex":324,"id":100,"name":"Test 001","obj":{"cost":3000},"rare":false} |
-- | {"id":200}                                                              |

-- @name: INSERT SELECT parses JSON object strings for schemaless rows
CREATE TABLE StringCopy
-- @expect: ok

INSERT INTO StringCopy SELECT '{"id":300,"name":"Selected"}'
-- @expect: ok

SELECT * FROM StringCopy
-- @expect: maps
-- | {"id":300,"name":"Selected"} |

DELETE FROM Item WHERE id > 100
-- @expect: ok

UPDATE Item
SET
    id = id + 1,
    rare = NOT rare
-- @expect: ok

SELECT id, name, dex, rare FROM Item
-- @expect:
-- | id: I64 | name: Str  | dex: I64 | rare: Bool |
-- | ------- | ---------- | -------- | ---------- |
-- | 101     | "Test 001" | 324      | true       |

UPDATE Item SET new_field = 'Hello'
-- @expect: ok

SELECT new_field, obj['cost'] AS cost FROM Item
-- @expect:
-- | new_field: Str | cost: I64 |
-- | -------------- | --------- |
-- | "Hello"        | 3000      |

SELECT
    Player.id AS player_id,
    Player.name AS player_name,
    Item.obj['cost'] AS item_cost
FROM Item
JOIN Player
WHERE flag IS NOT NULL;
-- @expect:
-- | player_id: I64 | player_name: Str | item_cost: I64 |
-- | -------------- | ---------------- | -------------- |
-- | 1001           | "Beam"           | 3000           |
