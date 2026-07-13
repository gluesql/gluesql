CREATE TABLE TableA (
    id INTEGER,
    test INTEGER,
    target_id INTEGER
);
-- expect: ok

INSERT INTO TableA (id, test, target_id) VALUES
    (1, 100, 2),
    (2, 100, 1),
    (3, 300, 5);
-- expect: ok

INSERT INTO TableA (target_id, id, test) VALUES (5, 3, 400);
-- expect: ok

INSERT INTO TableA (test, id, target_id) VALUES (500, 3, 4);
-- expect: ok

INSERT INTO TableA VALUES (4, 500, 3);
-- expect: ok

SELECT * FROM TableA;
-- expect: count 6

SELECT * FROM TableA WHERE id = 3;
-- expect: count 3

SELECT * FROM TableA WHERE id = (SELECT id FROM TableA WHERE id = 3 LIMIT 1)
-- expect: count 3

SELECT * FROM TableA WHERE id IN (1, 2, 4)
-- expect: count 3

SELECT * FROM TableA WHERE test IN (500, 300)
-- expect: count 3

SELECT * FROM TableA WHERE id IN (SELECT target_id FROM TableA LIMIT 3)
-- expect: count 2

SELECT * FROM TableA WHERE id = 3 AND test = 500;
-- expect: count 1

SELECT * FROM TableA WHERE id = 3 OR test = 100;
-- expect: count 5

SELECT * FROM TableA WHERE id != 3 AND test != 100;
-- expect: count 1

SELECT * FROM TableA WHERE id = 3 LIMIT 2;
-- expect: count 2

SELECT * FROM TableA LIMIT 10 OFFSET 2;
-- expect: count 4

SELECT * FROM TableA WHERE (id = 3 OR test = 100) AND test = 300;
-- expect: count 1

SELECT * FROM TableA a WHERE target_id = (SELECT id FROM TableA b WHERE b.target_id = a.id LIMIT 1);
-- expect: count 4

SELECT * FROM TableA a WHERE target_id = (SELECT id FROM TableA WHERE target_id = a.id LIMIT 1);
-- expect: count 4

SELECT * FROM TableA WHERE NOT (id = 3);
-- expect: count 3

UPDATE TableA SET test = 200 WHERE test = 100;
-- expect: count 2

SELECT * FROM TableA WHERE test = 100;
-- expect: count 0

SELECT * FROM TableA WHERE (test = 200);
-- expect: count 2

DELETE FROM TableA WHERE id != 3;
-- expect: count 3

SELECT * FROM TableA;
-- expect: count 3

DELETE FROM TableA;
-- expect: count 3

INSERT INTO TableA (id, test, target_id) VALUES
    (1, 100, 2),
    (2, 100, 1),
    (3, 300, 5);
-- expect: ok

INSERT INTO TableA (target_id, id, test) VALUES (5, 3, 400);
-- expect: ok

INSERT INTO TableA (test, id, target_id) VALUES (500, 3, 4);
-- expect: ok

INSERT INTO TableA VALUES (4, 500, 3);
-- expect: ok

SELECT id, test FROM TableA LIMIT 1;
-- expect:
-- | id: I64 | test: I64 |
-- | 1       | 100       |

SELECT id FROM TableA LIMIT 1;
-- expect:
-- | id: I64 |
-- | 1       |

SELECT * FROM TableA LIMIT 1;
-- expect:
-- | id: I64 | test: I64 | target_id: I64 |
-- | 1       | 100       | 2              |
