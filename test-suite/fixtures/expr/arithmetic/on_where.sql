CREATE TABLE Arith (
    id INTEGER,
    num INTEGER,
    name TEXT
);

-- expect: ok

DELETE FROM Arith

-- expect: ok

INSERT INTO Arith (id, num, name) VALUES
    (1, 6, 'A'),
    (2, 8, 'B'),
    (3, 4, 'C'),
    (4, 2, 'D'),
    (5, 3, 'E');

-- expect: ok

SELECT * FROM Arith WHERE id = 1 + 1;

-- expect: count 1

SELECT * FROM Arith WHERE id < id + 1;

-- expect: count 5

SELECT * FROM Arith WHERE id < num + id;

-- expect: count 5

SELECT * FROM Arith WHERE id + 1 < 5;

-- expect: count 3

SELECT * FROM Arith WHERE id = 2 - 1;

-- expect: count 1

SELECT * FROM Arith WHERE 2 - 1 = id;

-- expect: count 1

SELECT * FROM Arith WHERE id > id - 1;

-- expect: count 5

SELECT * FROM Arith WHERE id > id - num;

-- expect: count 5

SELECT * FROM Arith WHERE 5 - id < 3;

-- expect: count 3

SELECT * FROM Arith WHERE id = 2 * 2;

-- expect: count 1

SELECT * FROM Arith WHERE id > id * 2;

-- expect: count 0

SELECT * FROM Arith WHERE id > num * id;

-- expect: count 0

SELECT * FROM Arith WHERE 3 * id < 4;

-- expect: count 1

SELECT * FROM Arith WHERE id = 5 / 2;

-- expect: count 0

SELECT * FROM Arith WHERE id > id / 2;

-- expect: count 5

SELECT * FROM Arith WHERE id > num / id;

-- expect: count 3

SELECT * FROM Arith WHERE 10 / id = 2;

-- expect: count 2

SELECT * FROM Arith WHERE id = 5 % 2;

-- expect: count 1

SELECT * FROM Arith WHERE id > num % id;

-- expect: count 5

SELECT * FROM Arith WHERE num % id > 2;

-- expect: count 1

SELECT * FROM Arith WHERE num % 3 < 2 % id;

-- expect: count 2

SELECT * FROM Arith WHERE 1 + 1 = id;

-- expect: count 1

UPDATE Arith SET id = id + 1;

-- expect: count 5

SELECT * FROM Arith WHERE id = 1;

-- expect: count 0

UPDATE Arith SET id = id - 1 WHERE id != 6;

-- expect: count 4

SELECT * FROM Arith WHERE id <= 2;

-- expect: count 2

UPDATE Arith SET id = id * 2;

-- expect: count 5

UPDATE Arith SET id = id / 2;

-- expect: count 5

SELECT * FROM Arith WHERE id <= 2;

-- expect: count 2
