CREATE TABLE Foo (id INTEGER, name TEXT);
-- @expect: payload Create

INSERT INTO Foo VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- @expect: payload Insert
-- @json: 3

-- @name: An existing column can be renamed
ALTER TABLE Foo RENAME COLUMN id TO new_id;
-- @expect: payload AlterTable

-- @name: The renamed column and other columns retain their values
SELECT new_id, name FROM Foo ORDER BY new_id;
-- @expect:
-- | new_id: I64 | name: Str |
-- | ----------- | --------- |
-- | 1           | "a"       |
-- | 2           | "b"       |
-- | 3           | "c"       |

-- @name: Renaming a missing column fails
ALTER TABLE Foo RENAME COLUMN hello TO idid;
-- @expect: error AlterTable.RenamingColumnNotFound

-- @name: A column cannot be renamed to an existing column name
ALTER TABLE Foo RENAME COLUMN name TO new_id;
-- @expect: error AlterTable.AlreadyExistingColumn
-- @json: "new_id"

CREATE TABLE RequiredName (
    id INTEGER,
    name TEXT NOT NULL DEFAULT LOWER('APPLE')
);
-- @expect: payload Create

INSERT INTO RequiredName VALUES (10, 'pear');
-- @expect: payload Insert
-- @json: 1

-- @name: A NOT NULL column with a function default can be renamed
ALTER TABLE RequiredName RENAME COLUMN name TO label;
-- @expect: payload AlterTable

-- @name: Renaming preserves stored values without replacing them with the default
SELECT id, label FROM RequiredName ORDER BY id;
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 10      | "pear"     |

-- @name: Later inserts can omit the renamed defaulted column
INSERT INTO RequiredName (id) VALUES (20);
-- @expect: payload Insert
-- @json: 1

-- @name: New rows use the default retained by RENAME COLUMN
SELECT id, label FROM RequiredName WHERE id = 20;
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 20      | "apple"    |

-- @name: The renamed NOT NULL column rejects an explicit NULL
INSERT INTO RequiredName VALUES (30, NULL);
-- @expect: error Value.NullValueOnNotNullField

-- @name: A rejected NULL insert preserves existing and defaulted values
SELECT id, label FROM RequiredName ORDER BY id;
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 10      | "pear"     |
-- | 20      | "apple"    |

CREATE TABLE Documents;
-- @expect: payload Create

-- @name: Columns in a schemaless table cannot be renamed
ALTER TABLE Documents RENAME COLUMN name TO label;
-- @expect: error AlterTable.SchemalessTableFound
-- @json: "Documents"
