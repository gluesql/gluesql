CREATE TABLE Foo (
    id INTEGER,
    name TEXT NULL
);
-- expect: ok

INSERT INTO Foo (id, name) VALUES (1, 'Hello'), (2, Null);
-- expect: ok
