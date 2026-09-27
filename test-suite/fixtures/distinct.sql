CREATE TABLE Item (id INTEGER, name TEXT, price INTEGER)
-- @expect: ok

INSERT INTO Item VALUES (1, 'Apple', 100), (2, 'Banana', NULL), (1, 'Apple', 100), (3, NULL, 200)
-- @expect: ok

-- @name: DISTINCT single column
SELECT DISTINCT name FROM Item WHERE name IS NOT NULL ORDER BY name
-- @expect:
-- | name: Str |
-- | --------- |
-- | "Apple"   |
-- | "Banana"  |

-- @name: DISTINCT multiple columns
SELECT DISTINCT id, name FROM Item ORDER BY id
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Apple"   |
-- | 2       | "Banana"  |
-- | 3       | NULL      |

-- @name: ORDER BY then DISTINCT then OFFSET and LIMIT
SELECT DISTINCT name
FROM Item
WHERE name IS NOT NULL
ORDER BY name
OFFSET 1
LIMIT 1
-- @expect:
-- | name: Str |
-- | --------- |
-- | "Banana"  |

-- @name: DISTINCT terminal stages in derived query
SELECT *
FROM (
    SELECT DISTINCT name
    FROM Item
    WHERE name IS NOT NULL
    ORDER BY name
    OFFSET 1
    LIMIT 1
) AS DistinctItem
-- @expect:
-- | name: Str |
-- | --------- |
-- | "Banana"  |

-- @name: Derived DISTINCT preserves output labels through LIMIT
SELECT *
FROM (
    SELECT DISTINCT name
    FROM Item
    WHERE name = 'Apple'
    LIMIT 1
) AS LimitedDistinctItem
-- @expect:
-- | name: Str |
-- | --------- |
-- | "Apple"   |

-- @name: SELECT DISTINCT with DISTINCT aggregate
SELECT DISTINCT COUNT(DISTINCT id) AS count
FROM Item
ORDER BY count
LIMIT 1
-- @expect:
-- | count: I64 |
-- | ---------- |
-- | 3          |

CREATE TABLE DistinctItem (name TEXT)
-- @expect: ok

-- @name: INSERT SELECT uses DISTINCT terminal pipeline
INSERT INTO DistinctItem
SELECT DISTINCT name
FROM Item
WHERE name IS NOT NULL
ORDER BY name
OFFSET 1
LIMIT 1
-- @expect: ok

SELECT * FROM DistinctItem
-- @expect:
-- | name: Str |
-- | --------- |
-- | "Banana"  |

CREATE TABLE Restaurant (id INTEGER, menu MAP)
-- @expect: ok

INSERT INTO Restaurant VALUES
    (1, '{"dish": "pizza", "price": 12000}'),
    (2, '{"dish": "pizza", "price": 12000}'),
    (3, '{"dish": "pasta", "price": 15000}')
-- @expect: ok

-- @name: DISTINCT with Map menu data
SELECT DISTINCT menu FROM Restaurant ORDER BY UNWRAP(menu, 'price')
-- @expect:
-- | menu: Map                      |
-- | ------------------------------ |
-- | {"dish":"pizza","price":12000} |
-- | {"dish":"pasta","price":15000} |

CREATE TABLE FoodOrders
-- @expect: ok

INSERT INTO FoodOrders VALUES
    ('{"food": "burger", "quantity": 2}'),
    ('{"food": "burger", "quantity": 2}'),
    ('{"food": "chicken", "quantity": 1}')
-- @expect: ok

-- @name: DISTINCT with schemaless food orders (Row::Map case)
SELECT DISTINCT * FROM FoodOrders
-- @expect: maps
-- | {"food":"burger","quantity":2}  |
-- | {"food":"chicken","quantity":1} |
