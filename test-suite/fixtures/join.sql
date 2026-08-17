CREATE TABLE Player (
    id INTEGER,
    name TEXT
);
-- @expect: ok

CREATE TABLE Item (
    id INTEGER,
    quantity INTEGER,
    player_id INTEGER
);
-- @expect: ok

DELETE FROM Player
-- @expect: ok

DELETE FROM Item
-- @expect: ok

INSERT INTO Player (id, name) VALUES
    (1, 'Taehoon'),
    (2,    'Mike'),
    (3,   'Jorno'),
    (4,   'Berry'),
    (5,    'Hwan');
-- @expect: ok

INSERT INTO Item (id, quantity, player_id) VALUES
    (101, 1, 1),
    (102, 4, 2),
    (103, 9, 3),
    (104, 2, 3),
    (105, 1, 3),
    (106, 5, 1),
    (107, 2, 1),
    (108, 1, 5),
    (109, 1, 5),
    (110, 3, 3),
    (111, 4, 2),
    (112, 8, 1),
    (113, 7, 1),
    (114, 1, 1),
    (115, 2, 1);
-- @expect: ok

SELECT * FROM Item JOIN Player
-- @expect: count 75

SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id;
-- @expect: count 15

SELECT i.*, p.*
FROM Item i
LEFT JOIN Player p ON 1 = 2
WHERE i.id = 101;
-- @expect:
-- | id: I64 | quantity: I64 | player_id: I64 | id   | name |
-- | ------- | ------------- | -------------- | ---- | ---- |
-- | 101     | 1             | 1              | NULL | NULL |

-- @name: NULL condition produces an unmatched LEFT OUTER row
SELECT i.*, p.*
FROM Item i
LEFT JOIN Player p ON NULL
WHERE i.id = 101;
-- @expect:
-- | id: I64 | quantity: I64 | player_id: I64 | id   | name |
-- | ------- | ------------- | -------------- | ---- | ---- |
-- | 101     | 1             | 1              | NULL | NULL |

-- @name: NULL condition produces no INNER JOIN row
SELECT i.id
FROM Item i
INNER JOIN Player p ON NULL
WHERE i.id = 101;
-- @expect:
-- | id: I64 |
-- | ------- |

-- @name: Derived JOIN source propagates row errors
SELECT i.id
FROM Item i
LEFT JOIN (SELECT 1 / 0 AS bad) p ON TRUE
WHERE i.id = 101;
-- @expect: error Evaluate.DivisorShouldNotBeZero

-- @name: JOIN key expression propagates evaluation errors
SELECT i.id
FROM Item i
LEFT JOIN Player p ON p.id = i.player_id / 0
WHERE i.id = 101;
-- @expect: error Value.DivisorShouldNotBeZero

-- @name: earlier JOIN errors propagate through a later INNER JOIN
SELECT i.id
FROM Item i
JOIN (SELECT 1 / 0 AS bad) p ON TRUE
JOIN Player p2 ON TRUE
WHERE i.id = 101;
-- @expect: error Evaluate.DivisorShouldNotBeZero

-- @name: earlier JOIN errors propagate through a later LEFT OUTER JOIN
SELECT i.id
FROM Item i
JOIN (SELECT 1 / 0 AS bad) p ON TRUE
LEFT JOIN Player p2 ON TRUE
WHERE i.id = 101;
-- @expect: error Evaluate.DivisorShouldNotBeZero

-- @name: NULL equality keys do not match
SELECT i.id, p.id
FROM Item i
LEFT JOIN (VALUES (NULL)) AS p(id) ON p.id = i.player_id
WHERE i.id = 101;
-- @expect:
-- | id: I64 | id   |
-- | ------- | ---- |
-- | 101     | NULL |

-- @name: completed INNER JOIN feeds the next JOIN and aggregation
SELECT COUNT(*) AS count
FROM Item
INNER JOIN Player p
INNER JOIN Player p2;
-- @expect:
-- | count: I64 |
-- | ---------- |
-- | 375        |

-- @name: completed LEFT OUTER JOIN feeds the next JOIN and aggregation
SELECT COUNT(*) AS count
FROM Item
LEFT JOIN Player p
LEFT JOIN Player p2;
-- @expect:
-- | count: I64 |
-- | ---------- |
-- | 375        |

-- @name: explicit GROUP BY runs after LEFT OUTER JOIN
SELECT p.id, COUNT(*) AS count
FROM Item i
LEFT JOIN Player p ON p.id = i.player_id
GROUP BY p.id
ORDER BY p.id;
-- @expect:
-- | id: I64 | count: I64 |
-- | ------- | ---------- |
-- | 1       | 7          |
-- | 2       | 2          |
-- | 3       | 4          |
-- | 5       | 2          |

SELECT *
FROM Item
LEFT JOIN Player ON Player.id = Item.player_id
LEFT JOIN Player p1 ON p1.id = Item.player_id
WHERE Item.id = 101;
-- @expect:
-- | id: I64 | quantity: I64 | player_id: I64 | id: I64 | name: Str | id: I64 | name: Str |
-- | ------- | ------------- | -------------- | ------- | --------- | ------- | --------- |
-- | 101     | 1             | 1              | 1       | "Taehoon" | 1       | "Taehoon" |

SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE quantity = 1;
-- @expect: count 5

SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;
-- @expect: count 7

SELECT * FROM Item INNER JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;
-- @expect: count 7

SELECT * FROM Item
LEFT JOIN Player ON Player.id = Item.player_id
LEFT JOIN Player p1 ON p1.id = Item.player_id
LEFT JOIN Player p2 ON p2.id = Item.player_id
LEFT JOIN Player p3 ON p3.id = Item.player_id
LEFT JOIN Player p4 ON p4.id = Item.player_id
LEFT JOIN Player p5 ON p5.id = Item.player_id
LEFT JOIN Player p6 ON p6.id = Item.player_id
LEFT JOIN Player p7 ON p7.id = Item.player_id
LEFT JOIN Player p8 ON p8.id = Item.player_id
LEFT JOIN Player p9 ON p9.id = Item.player_id
WHERE Player.id = 1;
-- @expect: count 7

SELECT * FROM Item
LEFT JOIN Player ON Player.id = Item.player_id
LEFT JOIN Player p1 ON p1.id = Item.player_id
LEFT JOIN Player p2 ON p2.id = Item.player_id
LEFT JOIN Player p3 ON p3.id = Item.player_id
LEFT JOIN Player p4 ON p4.id = Item.player_id
LEFT JOIN Player p5 ON p5.id = Item.player_id
LEFT JOIN Player p6 ON p6.id = Item.player_id
LEFT JOIN Player p7 ON p7.id = Item.player_id
LEFT JOIN Player p8 ON p8.id = Item.player_id
INNER JOIN Player p9 ON p9.id = Item.player_id AND Item.id > 101
WHERE Player.id = 1;
-- @expect: count 6

SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id WHERE Item.quantity = 1;
-- @expect: count 5

SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id WHERE i.quantity = 1;
-- @expect: count 5

SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND p.id = 1;
-- @expect: count 15

SELECT * FROM Item i LEFT JOIN Player p ON p.id = i.player_id AND i.quantity = 1;
-- @expect: count 15

SELECT * FROM Item LEFT JOIN Player ON Player.id = Item.player_id AND Item.quantity = 1;
-- @expect: count 15

SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND p.id = 1;
-- @expect: count 7

SELECT * FROM Item i INNER JOIN Player p ON p.id = i.player_id AND p.id = 1;
-- @expect: count 7

SELECT * FROM Item i JOIN Player p ON p.id = i.player_id AND i.quantity = 1;
-- @expect: count 5

SELECT * FROM Player
INNER JOIN Item ON 1 = 2
INNER JOIN Item i2 ON 1 = 2
-- @expect: count 0

SELECT * FROM Item
LEFT JOIN Player ON Player.id = Item.player_id
WHERE Player.id = (SELECT id FROM Player LIMIT 1 OFFSET 0);
-- @expect: count 7

SELECT * FROM Item i1
LEFT JOIN Player ON Player.id = i1.player_id
WHERE Player.id = (SELECT id FROM Item i2 WHERE i2.id = i1.id)
-- @expect: count 0

SELECT * FROM Item i1
LEFT JOIN Player ON Player.id = i1.player_id
WHERE Player.id =
    (SELECT i2.id FROM Item i2
        JOIN Item i3 ON i3.id = i2.id
        WHERE
            i2.id = i1.id AND
            i3.id = i2.id AND
            i1.id = i3.id);
-- @expect: count 0

SELECT * FROM Item i1
LEFT JOIN Player ON Player.id = i1.player_id
WHERE Player.id IN
    (SELECT i2.player_id FROM Item i2
        JOIN Item i3 ON i3.id = i2.id
        WHERE Player.name = 'Jorno');
-- @expect: count 4

SELECT * FROM Player INNER JOIN Item ON Player.id = Item.player_id;
-- @expect: count 15

SELECT * FROM Player p1 LEFT JOIN Player p2 ON 1 = 1
-- @expect: count 25

SELECT * FROM Item INNER JOIN Item i2 ON i2.id IN (101, 103);
-- @expect: count 30

CREATE TABLE Trade (id INTEGER, item_id INTEGER, buyer_id INTEGER);
-- @expect: ok

INSERT INTO Trade VALUES (1, 101, 2), (2, 999, 3), (3, 102, 99), (4, 103, 1);
-- @expect: ok

-- @name: RIGHT JOIN preserves the right relation
SELECT * FROM Player RIGHT JOIN Item ON Player.id = Item.player_id;
-- @expect: count 15

-- @name: RIGHT JOIN keeps right rows that match nothing
SELECT * FROM Item RIGHT JOIN Player ON Player.id = Item.player_id;
-- @expect: count 16

-- @name: non-equality condition joins through a nested loop
SELECT * FROM Item RIGHT JOIN Player ON Player.id != Item.player_id;
-- @expect: count 60

-- @name: an always-false condition leaves every right row unmatched
SELECT * FROM Item RIGHT JOIN Player ON Item.quantity > 100;
-- @expect: count 5

-- @name: an empty left source still yields every right row
SELECT * FROM (SELECT * FROM Player WHERE FALSE) e RIGHT JOIN Item ON e.id = Item.player_id;
-- @expect: count 15

-- @name: aggregates count an unmatched right row as zero
SELECT Player.id, COUNT(Item.id) AS cnt
FROM Item RIGHT JOIN Player ON Player.id = Item.player_id
GROUP BY Player.id
ORDER BY Player.id;
-- @expect:
-- | id: I64 | cnt: I64 |
-- | ------- | -------- |
-- | 1       | 7        |
-- | 2       | 2        |
-- | 3       | 4        |
-- | 4       | 0        |
-- | 5       | 2        |

-- @name: WHERE filters the preserved relation after the join
SELECT * FROM Item RIGHT JOIN Player ON Player.id = Item.player_id WHERE Player.id = 1;
-- @expect: count 7

-- @name: RIGHT JOIN rejects a derived source correlated to the left relation
SELECT * FROM Player RIGHT JOIN (SELECT * FROM Item WHERE Item.player_id = Player.id) AS i ON true;
-- @expect: error Evaluate.CompoundIdentifierNotFound

-- @name: an unmatched right row NULL-extends the whole accumulated prefix
SELECT *
FROM Player JOIN Item ON Item.player_id = Player.id
RIGHT JOIN Trade ON Trade.item_id = Item.id
WHERE Trade.id IN (1, 2)
ORDER BY Trade.id;
-- @expect:
-- | id: I64 | name: Str | id: I64 | quantity: I64 | player_id: I64 | id: I64 | item_id: I64 | buyer_id: I64 |
-- | ------- | --------- | ------- | ------------- | -------------- | ------- | ------------ | ------------- |
-- | 1       | "Taehoon" | 101     | 1             | 1              | 1       | 101          | 2             |
-- | NULL    | NULL      | NULL    | NULL          | NULL           | 2       | 999          | 3             |

-- @name: a NULL-extended row flows into a later JOIN like any other row
SELECT Trade.id, buyer.id
FROM Player JOIN Item ON Item.player_id = Player.id
RIGHT JOIN Trade ON Trade.item_id = Item.id
JOIN Player buyer ON buyer.id = Trade.buyer_id
ORDER BY Trade.id;
-- @expect:
-- | id: I64 | id: I64 |
-- | ------- | ------- |
-- | 1       | 2       |
-- | 2       | 3       |
-- | 4       | 1       |

-- @name: chained RIGHT JOINs treat an already NULL-extended row as matched
SELECT p.id, Item.id, Trade.id
FROM (SELECT * FROM Player WHERE id <= 2) p
RIGHT JOIN Item ON Item.player_id = p.id
RIGHT JOIN Trade ON Trade.item_id = Item.id
ORDER BY Trade.id;
-- @expect:
-- | id: I64 | id: I64 | id: I64 |
-- | ------- | ------- | ------- |
-- | 1       | 101     | 1       |
-- | NULL    | NULL    | 2       |
-- | 2       | 102     | 3       |
-- | NULL    | 103     | 4       |

-- @name: a derived source hides a RIGHT JOIN from the outer query
SELECT d.pid, d.iid
FROM (
    SELECT Player.id AS pid, Item.id AS iid
    FROM (SELECT * FROM Player WHERE id <= 2) Player
    RIGHT JOIN Item ON Item.player_id = Player.id
) d
WHERE d.iid IN (101, 103)
ORDER BY d.iid;
-- @expect:
-- | pid: I64 | iid: I64 |
-- | -------- | -------- |
-- | 1        | 101      |
-- | NULL     | 103      |

-- @name: an expression subquery hides a RIGHT JOIN from the outer query
SELECT id FROM Player
WHERE id IN (
    SELECT Trade.buyer_id
    FROM Item RIGHT JOIN Trade ON Trade.item_id = Item.id
    WHERE Item.id IS NULL
)
ORDER BY id;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 3       |

-- @name: an alias used twice on the left still NULL-extends both relations
-- The plan lists the left relations as ["a", "a"], so pairing them with the executor's sources by
-- alias would resolve both to Player and lose Item's columns. Pairing by position keeps them apart:
-- every row below must carry 2 NULLs for Player and 3 for Item.
SELECT * FROM Player a JOIN Item a ON TRUE RIGHT JOIN Trade t ON FALSE ORDER BY t.id;
-- @expect:
-- | id: I64 | name: Str | id: I64 | quantity: I64 | player_id: I64 | id: I64 | item_id: I64 | buyer_id: I64 |
-- | ------- | --------- | ------- | ------------- | -------------- | ------- | ------------ | ------------- |
-- | NULL    | NULL      | NULL    | NULL          | NULL           | 1       | 101          | 2             |
-- | NULL    | NULL      | NULL    | NULL          | NULL           | 2       | 999          | 3             |
-- | NULL    | NULL      | NULL    | NULL          | NULL           | 3       | 102          | 99            |
-- | NULL    | NULL      | NULL    | NULL          | NULL           | 4       | 103          | 1             |

-- @name: a select-item subquery hides a RIGHT JOIN from the outer query
SELECT
    Player.id,
    (
        SELECT COUNT(*)
        FROM Item RIGHT JOIN Trade ON Trade.item_id = Item.id
        WHERE Item.id IS NULL
    ) AS orphan_trades
FROM Player
WHERE Player.id = 1;
-- @expect:
-- | id: I64 | orphan_trades: I64 |
-- | ------- | ------------------ |
-- | 1       | 1                  |

DELETE FROM Player
-- @expect: ok

DELETE FROM Item
-- @expect: ok

DROP TABLE Trade
-- @expect: ok
