CREATE TABLE Foo (id INTEGER);
-- @expect: payload Create

INSERT INTO Foo VALUES (1), (2);
-- @expect: payload Insert
-- @json: 2

-- @name: ADD COLUMN supports a constant default
ALTER TABLE Foo ADD COLUMN amount INTEGER DEFAULT 10;
-- @expect: payload AlterTable

-- @name: Existing rows receive the constant default
SELECT * FROM Foo;
-- @expect:
-- | id: I64 | amount: I64 |
-- | ------- | ----------- |
-- | 1       | 10          |
-- | 2       | 10          |

-- @name: Nullable columns can be added without a default
ALTER TABLE Foo ADD COLUMN opt BOOLEAN NULL;
-- @expect: payload AlterTable

-- @name: Existing rows receive NULL when no default is specified
SELECT * FROM Foo;
-- @expect:
-- | id: I64 | amount: I64 | opt  |
-- | ------- | ----------- | ---- |
-- | 1       | 10          | NULL |
-- | 2       | 10          | NULL |

-- @name: Nullable columns can also have a default
ALTER TABLE Foo ADD COLUMN opt2 BOOLEAN NULL DEFAULT true;
-- @expect: payload AlterTable

-- @name: Existing rows receive the Boolean default
SELECT * FROM Foo;
-- @expect:
-- | id: I64 | amount: I64 | opt  | opt2: Bool |
-- | ------- | ----------- | ---- | ---------- |
-- | 1       | 10          | NULL | true       |
-- | 2       | 10          | NULL | true       |

CREATE TABLE Defaulted (id INTEGER);
-- @expect: payload Create

INSERT INTO Defaulted VALUES (10), (20);
-- @expect: payload Insert
-- @json: 2

-- @name: NOT NULL columns can be added with a function default
ALTER TABLE Defaulted ADD COLUMN label TEXT NOT NULL DEFAULT LOWER('APPLE');
-- @expect: payload AlterTable

-- @name: Existing rows receive the evaluated function default
SELECT id, label FROM Defaulted ORDER BY id;
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 10      | "apple"    |
-- | 20      | "apple"    |

-- @name: Later inserts can omit the added defaulted column
INSERT INTO Defaulted (id) VALUES (30);
-- @expect: payload Insert
-- @json: 1

-- @name: New rows use the default retained by ADD COLUMN
SELECT id, label FROM Defaulted WHERE id = 30;
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 30      | "apple"    |

-- @name: The added NOT NULL column rejects an explicit NULL
INSERT INTO Defaulted VALUES (40, NULL);
-- @expect: error Value.NullValueOnNotNullField

-- @name: A rejected NULL insert leaves the existing rows unchanged
SELECT id, label FROM Defaulted ORDER BY id;
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 10      | "apple"    |
-- | 20      | "apple"    |
-- | 30      | "apple"    |

-- @name: Adding a NOT NULL column requires a default
ALTER TABLE Foo ADD COLUMN required INTEGER NOT NULL;
-- @expect: error Alter.DefaultValueRequired
-- @json:
-- {
--   "comment": null,
--   "data_type": "Int",
--   "default": null,
--   "name": "required",
--   "nullable": false,
--   "unique": null
-- }

-- @name: ADD COLUMN rejects an existing column name
ALTER TABLE Foo ADD COLUMN id INTEGER;
-- @expect: error AlterTable.AlreadyExistingColumn
-- @json: "id"

-- @name: Column defaults cannot contain an EXISTS subquery
ALTER TABLE Foo ADD COLUMN something BOOLEAN DEFAULT EXISTS (
    SELECT id FROM Bar LIMIT 1
);
-- @expect: error Evaluate.ExistsSubqueryNotAllowedInStatelessExpr

-- @name: ADD COLUMN rejects an unsupported data type
ALTER TABLE Foo ADD COLUMN something SOMEWHAT;
-- @expect: error Translate.UnsupportedDataType
-- @json: "SOMEWHAT"

-- @name: FLOAT columns cannot have a UNIQUE constraint
ALTER TABLE Foo ADD COLUMN something FLOAT UNIQUE;
-- @expect: error Alter.UnsupportedDataTypeForUniqueColumn
-- @json:
-- [
--   "something",
--   "Float"
-- ]

-- @name: DROP COLUMN removes an existing column
ALTER TABLE Foo DROP COLUMN amount;
-- @expect: payload AlterTable

-- @name: Dropping a column preserves the other columns and their values
SELECT * FROM Foo;
-- @expect:
-- | id: I64 | opt  | opt2: Bool |
-- | ------- | ---- | ---------- |
-- | 1       | NULL | true       |
-- | 2       | NULL | true       |

-- @name: Dropping a missing column fails without IF EXISTS
ALTER TABLE Foo DROP COLUMN something;
-- @expect: error AlterTable.DroppingColumnNotFound
-- @json: "something"

-- @name: IF EXISTS ignores a missing column
ALTER TABLE Foo DROP COLUMN IF EXISTS something;
-- @expect: payload AlterTable

-- @name: IF EXISTS still drops a column that exists
ALTER TABLE Foo DROP COLUMN IF EXISTS opt2;
-- @expect: payload AlterTable

-- @name: IF EXISTS preserves the remaining rows and columns
SELECT * FROM Foo;
-- @expect:
-- | id: I64 | opt  |
-- | ------- | ---- |
-- | 1       | NULL |
-- | 2       | NULL |

-- @name: ALTER TABLE does not support adding a PRIMARY KEY constraint
ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf);
-- @expect: error Translate.UnsupportedAlterTableOperation
-- @json: "ADD CONSTRAINT \"hey\" PRIMARY KEY (asdf)"

-- @name: ALTER TABLE does not support adding a UNIQUE constraint
ALTER TABLE Foo ADD CONSTRAINT hello UNIQUE (id);
-- @expect: error Translate.UnsupportedAlterTableOperation
-- @json: "ADD CONSTRAINT hello UNIQUE (id)"

CREATE TABLE Referenced (id INTEGER PRIMARY KEY);
-- @expect: payload Create

CREATE TABLE Referencing (
    id INTEGER,
    referenced_id INTEGER,
    FOREIGN KEY (referenced_id) REFERENCES Referenced (id)
);
-- @expect: payload Create

-- @name: Columns referenced by a foreign key cannot be dropped
ALTER TABLE Referenced DROP COLUMN id;
-- @expect: error Alter.CannotAlterReferencedColumn
-- @json:
-- {
--   "referencing": {
--     "foreign_key": {
--       "name": "FK_referenced_id-Referenced_id",
--       "on_delete": "NoAction",
--       "on_update": "NoAction",
--       "referenced_column_name": "id",
--       "referenced_table_name": "Referenced",
--       "referencing_column_name": "referenced_id"
--     },
--     "table_name": "Referencing"
--   }
-- }

-- @name: Columns referenced by a foreign key cannot be renamed
ALTER TABLE Referenced RENAME COLUMN id TO new_id;
-- @expect: error Alter.CannotAlterReferencedColumn
-- @json:
-- {
--   "referencing": {
--     "foreign_key": {
--       "name": "FK_referenced_id-Referenced_id",
--       "on_delete": "NoAction",
--       "on_update": "NoAction",
--       "referenced_column_name": "id",
--       "referenced_table_name": "Referenced",
--       "referencing_column_name": "referenced_id"
--     },
--     "table_name": "Referencing"
--   }
-- }

-- @name: Foreign key columns cannot be dropped
ALTER TABLE Referencing DROP COLUMN referenced_id;
-- @expect: error Alter.CannotAlterReferencingColumn
-- @json:
-- {
--   "referencing": {
--     "foreign_key": {
--       "name": "FK_referenced_id-Referenced_id",
--       "on_delete": "NoAction",
--       "on_update": "NoAction",
--       "referenced_column_name": "id",
--       "referenced_table_name": "Referenced",
--       "referencing_column_name": "referenced_id"
--     },
--     "table_name": "Referencing"
--   }
-- }

-- @name: Foreign key columns cannot be renamed
ALTER TABLE Referencing RENAME COLUMN referenced_id TO new_id;
-- @expect: error Alter.CannotAlterReferencingColumn
-- @json:
-- {
--   "referencing": {
--     "foreign_key": {
--       "name": "FK_referenced_id-Referenced_id",
--       "on_delete": "NoAction",
--       "on_update": "NoAction",
--       "referenced_column_name": "id",
--       "referenced_table_name": "Referenced",
--       "referencing_column_name": "referenced_id"
--     },
--     "table_name": "Referencing"
--   }
-- }
