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

DELETE FROM Player
-- @expect: ok

DELETE FROM Item
-- @expect: ok
