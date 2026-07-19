CREATE TABLE Operator (
    id INTEGER,
    name TEXT
);
-- @expect: ok

DELETE FROM Operator
-- @expect: ok

INSERT INTO Operator (id, name) VALUES
    (1, 'Abstract'),
    (2,    'Azzzz'),
    (3,     'July'),
    (4,    'Romeo'),
    (5,    'Trade');
-- @expect: ok

SELECT * FROM Operator WHERE id < 2;
-- @expect: count 1

SELECT * FROM Operator WHERE id <= 2;
-- @expect: count 2

SELECT * FROM Operator WHERE id > 2;
-- @expect: count 3

SELECT * FROM Operator WHERE id >= 2;
-- @expect: count 4

SELECT * FROM Operator WHERE 2 > id;
-- @expect: count 1

SELECT * FROM Operator WHERE 2 >= id;
-- @expect: count 2

SELECT * FROM Operator WHERE 2 < id;
-- @expect: count 3

SELECT * FROM Operator WHERE 2 <= id;
-- @expect: count 4

SELECT * FROM Operator WHERE 1 < 3;
-- @expect: count 5

SELECT * FROM Operator WHERE 3 >= 3;
-- @expect: count 5

SELECT * FROM Operator WHERE 3 > 3;
-- @expect: count 0

SELECT * FROM Operator o1 WHERE 3 > (SELECT MIN(id) FROM Operator WHERE o1.id < 100);
-- @expect: count 5

SELECT * FROM Operator WHERE name < 'Azzzzzzzzzz';
-- @expect: count 2

SELECT * FROM Operator WHERE name < 'Az';
-- @expect: count 1

SELECT * FROM Operator WHERE name < 'zz';
-- @expect: count 5

SELECT * FROM Operator WHERE 'aa' < 'zz';
-- @expect: count 5

SELECT * FROM Operator WHERE 'Romeo' >= name;
-- @expect: count 4

SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) >= name
-- @expect: count 1

SELECT * FROM Operator WHERE name <= (SELECT name FROM Operator LIMIT 1)
-- @expect: count 1

SELECT * FROM Operator WHERE 'zz' > (SELECT name FROM Operator LIMIT 1)
-- @expect: count 5

SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) < 'zz'
-- @expect: count 5

SELECT * FROM Operator WHERE NOT (1 != 1);
-- @expect: count 5

select 1 < 'a' as test
-- @expect:
-- | test: Bool |
-- | false      |

select 1 >= 'a' as test
-- @expect:
-- | test: Bool |
-- | false      |

select 1 = 'a' as test
-- @expect:
-- | test: Bool |
-- | false      |
