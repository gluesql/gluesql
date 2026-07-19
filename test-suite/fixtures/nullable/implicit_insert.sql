CREATE TABLE Foo (
    id INTEGER,
    name TEXT NULL
);
-- @expect: ok

INSERT INTO Foo (id) VALUES (1)
-- @expect: ok

SELECT id, name FROM Foo
-- @expect:
-- | id: I64 | name |
-- | 1       | NULL |
