CREATE TABLE Player (
    id INTEGER,
    name TEXT
);
-- expect: ok

CREATE TABLE Request (
    id INTEGER,
    quantity INTEGER,
    user_id INTEGER
);
-- expect: ok

INSERT INTO Player (id, name) VALUES
    (1, 'Taehoon'),
    (2,    'Mike'),
    (3,   'Jorno'),
    (4,   'Berry'),
    (5,    'Hwan');
-- expect: ok

INSERT INTO Request (id, quantity, user_id) VALUES
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
-- expect: ok

SELECT * FROM Request WHERE quantity IN (5, 1);
-- expect: count 6

SELECT * FROM Request WHERE quantity NOT IN (5, 1);
-- expect: count 9

SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE id = 3);
-- expect: count 4

SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request);
-- expect: count 4

SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id = Player.id);
-- expect: count 4

SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id IN (Player.id));
-- expect: count 4

SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE quantity IN (6, 7, 8, 9));
-- expect: count 2

SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE name IN ('Taehoon', 'Hwan'));
-- expect: count 9

SELECT * FROM Player WHERE id = (SELECT id FROM Player WHERE id = 9)
-- expect:
-- | id | name |

SELECT (SELECT N FROM SERIES(3) WHERE N = 4) N
-- expect:
-- | N    |
-- | NULL |

SELECT id FROM Player WHERE id IN (SELECT id, name FROM Player)
-- expect: error Evaluate.InSubqueryMustReturnOneColumn

SELECT id FROM Player WHERE id IN (SELECT id, name FROM Player WHERE id = 0)
-- expect: error Evaluate.InSubqueryMustReturnOneColumn
