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
