CREATE TABLE ReferencedTableWithoutPK (
    id INTEGER,
    name TEXT
);
-- @expect: ok

-- @name: The referenced column must be a primary key
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithoutPK (id)
);
-- @expect: error Alter.ReferencingNonPKColumn
-- @json:
-- {
--   "referenced_column": "id",
--   "referenced_table": "ReferencedTableWithoutPK"
-- }

CREATE TABLE ReferencedTableWithUnique (
    id INTEGER UNIQUE,
    name TEXT
);
-- @expect: ok

-- @name: A UNIQUE constraint alone cannot be referenced
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithUnique (id)
);
-- @expect: error Alter.ReferencingNonPKColumn
-- @json:
-- {
--   "referenced_column": "id",
--   "referenced_table": "ReferencedTableWithUnique"
-- }

CREATE TABLE ReferencedTableWithPK (
    id INTEGER PRIMARY KEY,
    name TEXT
);
-- @expect: ok

-- @name: Referencing and referenced columns must have matching types
CREATE TABLE ReferencingTable (
    id TEXT,
    name TEXT,
    referenced_id TEXT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
);
-- @expect: error Alter.ForeignKeyDataTypeMismatch
-- @json:
-- {
--   "referenced_column": "id",
--   "referenced_column_type": "Int",
--   "referencing_column": "referenced_id",
--   "referencing_column_type": "Text"
-- }

-- @name: ON DELETE CASCADE is unsupported
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE CASCADE
);
-- @expect: error Translate.UnsupportedConstraint
-- @json: "CASCADE"

-- @name: ON DELETE SET DEFAULT is unsupported
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET DEFAULT
);
-- @expect: error Translate.UnsupportedConstraint
-- @json: "SET DEFAULT"

-- @name: ON DELETE SET NULL is unsupported
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET NULL
);
-- @expect: error Translate.UnsupportedConstraint
-- @json: "SET NULL"

-- @name: The referencing column must exist
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (wrong_referencing_column) REFERENCES ReferencedTableWithPK (id)
);
-- @expect: error Alter.ReferencingColumnNotFound
-- @json: "wrong_referencing_column"

-- @name: The referenced column must exist
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (wrong_referenced_column)
);
-- @expect: error Alter.ReferencedColumnNotFound
-- @json: "wrong_referenced_column"

-- @name: Foreign keys support NO ACTION and RESTRICT with a matching primary key
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    CONSTRAINT MyFkConstraint
        FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        ON DELETE NO ACTION ON UPDATE RESTRICT
);
-- @expect: payload Create

-- @name: A table can reference its own primary key
CREATE TABLE SelfReferencingTable (
    id INTEGER PRIMARY KEY,
    name TEXT,
    referenced_id INTEGER,
    FOREIGN KEY (referenced_id) REFERENCES SelfReferencingTable (id)
);
-- @expect: payload Create

CREATE TABLE SelfDefinition (id TEXT PRIMARY KEY);
-- @expect: ok

-- @name: IF NOT EXISTS checks self references against the new definition
CREATE TABLE IF NOT EXISTS SelfDefinition (
    id INT PRIMARY KEY,
    parent_id INT,
    FOREIGN KEY (parent_id) REFERENCES SelfDefinition (id)
);
-- @expect: payload Create

-- @name: The existing table keeps its TEXT primary key
INSERT INTO SelfDefinition VALUES ('existing');
-- @expect: payload Insert
-- @json: 1

SELECT * FROM SelfDefinition;
-- @expect:
-- | id: Str    |
-- | ---------- |
-- | "existing" |

-- @name: IF NOT EXISTS still checks foreign keys to other tables
CREATE TABLE IF NOT EXISTS SelfDefinition (
    id INT,
    FOREIGN KEY (id) REFERENCES MissingFKParent (id)
);
-- @expect: error Alter.ReferencedTableNotFound
-- @json: "MissingFKParent"
