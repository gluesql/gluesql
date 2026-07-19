CREATE TABLE Foo (id INTEGER);
-- @expect: payload Create

INSERT INTO Foo VALUES (1), (2);
-- @expect: payload Insert
-- @json: 2

SELECT * FROM Foo;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 2       |

ALTER TABLE Foo ADD COLUMN amount INTEGER NOT NULL
-- @expect: error AlterTable.DefaultValueRequired
-- @json:
-- {
--   "comment": null,
--   "data_type": "Int",
--   "default": null,
--   "name": "amount",
--   "nullable": false,
--   "unique": null
-- }

ALTER TABLE Foo ADD COLUMN id INTEGER
-- @expect: error AlterTable.AlreadyExistingColumn
-- @json: "id"

ALTER TABLE Foo ADD COLUMN amount INTEGER DEFAULT 10
-- @expect: payload AlterTable

SELECT * FROM Foo;
-- @expect:
-- | id: I64 | amount: I64 |
-- | ------- | ----------- |
-- | 1       | 10          |
-- | 2       | 10          |

ALTER TABLE Foo ADD COLUMN opt BOOLEAN NULL
-- @expect: payload AlterTable

SELECT * FROM Foo;
-- @expect:
-- | id: I64 | amount: I64 | opt  |
-- | ------- | ----------- | ---- |
-- | 1       | 10          | NULL |
-- | 2       | 10          | NULL |

ALTER TABLE Foo ADD COLUMN opt2 BOOLEAN NULL DEFAULT true
-- @expect: payload AlterTable

SELECT * FROM Foo;
-- @expect:
-- | id: I64 | amount: I64 | opt  | opt2: Bool |
-- | ------- | ----------- | ---- | ---------- |
-- | 1       | 10          | NULL | true       |
-- | 2       | 10          | NULL | true       |

ALTER TABLE Foo ADD COLUMN something BOOLEAN DEFAULT EXISTS (SELECT id FROM Bar LIMIT 1)
-- @expect: error Evaluate.ExistsSubqueryNotAllowedInStatelessExpr

ALTER TABLE Foo ADD COLUMN something SOMEWHAT
-- @expect: error Translate.UnsupportedDataType
-- @json: "SOMEWHAT"

ALTER TABLE Foo ADD COLUMN something FLOAT UNIQUE
-- @expect: error Alter.UnsupportedDataTypeForUniqueColumn
-- @json:
-- [
--   "something",
--   "Float"
-- ]

ALTER TABLE Foo DROP COLUMN IF EXISTS something;
-- @expect: payload AlterTable

ALTER TABLE Foo DROP COLUMN something;
-- @expect: error AlterTable.DroppingColumnNotFound
-- @json: "something"

ALTER TABLE Foo DROP COLUMN amount;
-- @expect: payload AlterTable

SELECT * FROM Foo;
-- @expect:
-- | id: I64 | opt  | opt2: Bool |
-- | ------- | ---- | ---------- |
-- | 1       | NULL | true       |
-- | 2       | NULL | true       |

ALTER TABLE Foo DROP COLUMN IF EXISTS opt2;
-- @expect: payload AlterTable

SELECT * FROM Foo;
-- @expect:
-- | id: I64 | opt  |
-- | ------- | ---- |
-- | 1       | NULL |
-- | 2       | NULL |

ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf);
-- @expect: error Translate.UnsupportedAlterTableOperation
-- @json: "ADD CONSTRAINT \"hey\" PRIMARY KEY (asdf)"

ALTER TABLE Foo ADD CONSTRAINT hello UNIQUE (id)
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

ALTER TABLE Referenced DROP COLUMN id
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

ALTER TABLE Referenced RENAME COLUMN id to new_id
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

ALTER TABLE Referencing DROP COLUMN referenced_id
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

ALTER TABLE Referencing RENAME COLUMN referenced_id to new_id
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
