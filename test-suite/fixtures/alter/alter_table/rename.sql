CREATE TABLE Foo (id INTEGER, name TEXT);

-- expect: payload Create

INSERT INTO Foo VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- expect: payload Insert
-- 3

SELECT id FROM Foo

-- expect:
-- | id: I64 |
-- | 1       |
-- | 2       |
-- | 3       |

ALTER TABLE Foo2 RENAME TO Bar;

-- expect: error AlterTable.TableNotFound
-- "Foo2"

ALTER TABLE Foo RENAME TO Bar;

-- expect: payload AlterTable

SELECT id FROM Bar

-- expect:
-- | id: I64 |
-- | 1       |
-- | 2       |
-- | 3       |

ALTER TABLE Bar RENAME COLUMN id TO new_id

-- expect: payload AlterTable

SELECT new_id FROM Bar

-- expect:
-- | new_id: I64 |
-- | 1           |
-- | 2           |
-- | 3           |

ALTER TABLE Bar RENAME COLUMN hello TO idid

-- expect: error AlterTable.RenamingColumnNotFound

ALTER TABLE Bar RENAME COLUMN name TO new_id

-- expect: error AlterTable.AlreadyExistingColumn
-- "new_id"
