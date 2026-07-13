CREATE TABLE Item (
    id INTEGER,
    content TEXT,
    verified BOOLEAN,
    ratio FLOAT
);
-- expect: ok

INSERT INTO Item
        (id,   content, verified, ratio)
    VALUES
        ( 1, 'Hello',     True,   0.1),
        ( 1, 'World',    False,   0.9),
        ( 1, 'test',    False,   0.0);
-- expect: ok

SELECT * FROM Item;
-- expect: count 3

SELECT * FROM Item WHERE verified = True;
-- expect: count 1

SELECT * FROM Item WHERE ratio > 0.5;
-- expect: count 1

SELECT * FROM Item WHERE ratio = 0.1;
-- expect: count 1

UPDATE Item SET content='Foo' WHERE content='World';
-- expect: count 1

SELECT * FROM Item WHERE content='World';
-- expect: count 0

SELECT * FROM Item WHERE content='Foo';
-- expect: count 1

SELECT * FROM Item WHERE content='Foo';
-- expect: count 1

UPDATE Item SET id = 11 WHERE content='Foo';
-- expect: count 1

UPDATE Item SET id = 14 WHERE content='Foo';
-- expect: count 1

SELECT * FROM Item;
-- expect: count 3

DELETE FROM Item
-- expect: ok
