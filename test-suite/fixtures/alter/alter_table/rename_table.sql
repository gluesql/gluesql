CREATE TABLE Foo (id INTEGER, name TEXT);
-- @expect: payload Create

INSERT INTO Foo VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- @expect: payload Insert
-- @json: 3

-- @name: A table can be renamed
ALTER TABLE Foo RENAME TO Bar;
-- @expect: payload AlterTable

-- @name: The renamed table retains its rows
SELECT id, name FROM Bar ORDER BY id;
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "a"       |
-- | 2       | "b"       |
-- | 3       | "c"       |

-- @name: Renaming a missing table fails
ALTER TABLE Foo2 RENAME TO MissingRenamed;
-- @expect: error AlterTable.TableNotFound
-- @json: "Foo2"
