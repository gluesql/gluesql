CREATE TABLE ReferencedTableWithoutPK (
    id INTEGER,
    name TEXT
);
-- @expect: ok

-- @name: Creating table with foreign key should be failed if referenced table does not have primary key
CREATE TABLE ReferencingTable (
    id INT, name TEXT,
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

-- @name: Creating table with foreign key should be failed if referenced table has only Unique constraint
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

-- @name: Creating table with foreign key on different data type should be failed
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

-- @name: Unsupported foreign key option: CASCADE
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE CASCADE
);
-- @expect: error Translate.UnsupportedConstraint
-- @json: "CASCADE"

-- @name: Unsupported foreign key option: SET DEFAULT
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET DEFAULT
);
-- @expect: error Translate.UnsupportedConstraint
-- @json: "SET DEFAULT"

-- @name: Unsupported foreign key option: SET NULL
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET NULL
);
-- @expect: error Translate.UnsupportedConstraint
-- @json: "SET NULL"

-- @name: Referencing column not found
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (wrong_referencing_column) REFERENCES ReferencedTableWithPK (id)
);
-- @expect: error Alter.ReferencingColumnNotFound
-- @json: "wrong_referencing_column"

-- @name: Referenced column not found
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (wrong_referenced_column)
);
-- @expect: error Alter.ReferencedColumnNotFound
-- @json: "wrong_referenced_column"

-- @name: Creating table with foreign key should be succeeded if referenced table has primary key. NO ACTION(=RESTRICT) is default
CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    CONSTRAINT MyFkConstraint FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE NO ACTION ON UPDATE RESTRICT
);
-- @expect: payload Create

-- @name: If there is no referenced value, insert should fail
INSERT INTO ReferencingTable VALUES (1, 'orphan', 1);
-- @expect: error Insert.CannotFindReferencedValue
-- @json:
-- {
--   "column_name": "id",
--   "referenced_value": "1",
--   "table_name": "ReferencedTableWithPK"
-- }

-- @name: Even If there is no referenced value, NULL should be inserted
INSERT INTO ReferencingTable VALUES (1, 'Null is independent', NULL);
-- @expect: payload Insert
-- @json: 1

INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1');
-- @expect: ok

-- @name: With valid referenced value, insert should succeed
INSERT INTO ReferencingTable VALUES (2, 'referencing_table with referenced_table', 1);
-- @expect: payload Insert
-- @json: 1

-- @name: If there is no referenced value, update should fail
UPDATE ReferencingTable SET referenced_id = 2 WHERE id = 2;
-- @expect: error Update.CannotFindReferencedValue
-- @json:
-- {
--   "column_name": "id",
--   "referenced_value": "2",
--   "table_name": "ReferencedTableWithPK"
-- }

-- @name: Even If there is no referenced value, it should be able to update to NULL
UPDATE ReferencingTable SET referenced_id = NULL WHERE id = 2;
-- @expect: payload Update
-- @json: 1

-- @name: With valid referenced value, update should succeed
UPDATE ReferencingTable SET referenced_id = 1 WHERE id = 2;
-- @expect: payload Update
-- @json: 1

INSERT INTO ReferencedTableWithPK VALUES (2, 'unreferenced row');
-- @expect: ok

-- @name: Deleting referenced row should fail if referencing value exists (by default: NO ACTION and gets error)
DELETE FROM ReferencedTableWithPK WHERE id = 1;
-- @expect: error Delete.ReferencingColumnExists
-- @json: "ReferencingTable.referenced_id"

-- @name: Deleting unreferenced row should succeed even if referencing table is not empty
DELETE FROM ReferencedTableWithPK WHERE id = 2;
-- @expect: payload Delete
-- @json: 1

-- @name: Deleting referencing table does not care referenced table
DELETE FROM ReferencingTable WHERE id = 2;
-- @expect: payload Delete
-- @json: 1

CREATE TABLE ReferencedTableWithPK_2 (
    id INTEGER PRIMARY KEY,
    name TEXT
);
-- @expect: ok

INSERT INTO ReferencedTableWithPK_2 VALUES (1, 'referenced_table2');
-- @expect: ok

-- @name: Table with two foreign keys
CREATE TABLE ReferencingWithTwoFK (
    id INTEGER PRIMARY KEY,
    name TEXT,
    referenced_id_1 INTEGER,
    referenced_id_2 INTEGER,
    FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id),
    FOREIGN KEY (referenced_id_2) REFERENCES ReferencedTableWithPK_2 (id)
);
-- @expect: payload Create

INSERT INTO ReferencingWithTwoFK VALUES (1, 'referencing_table with two referenced_table', 1, 1);
-- @expect: ok

-- @name: Cannot update referenced_id_2 if there is no referenced value
UPDATE ReferencingWithTwoFK SET referenced_id_2 = 9 WHERE id = 1;
-- @expect: error Update.CannotFindReferencedValue
-- @json:
-- {
--   "column_name": "id",
--   "referenced_value": "9",
--   "table_name": "ReferencedTableWithPK_2"
-- }

-- @name: Cannot drop referenced table if referencing table exists
DROP TABLE ReferencedTableWithPK;
-- @expect: error Alter.CannotDropTableWithReferencing
-- @json:
-- {
--   "referenced_table_name": "ReferencedTableWithPK",
--   "referencings": [
--     {
--       "foreign_key": {
--         "name": "MyFkConstraint",
--         "on_delete": "NoAction",
--         "on_update": "NoAction",
--         "referenced_column_names": ["id"],
--         "referenced_table_name": "ReferencedTableWithPK",
--         "referencing_column_names": ["referenced_id"]
--       },
--       "table_name": "ReferencingTable"
--     },
--     {
--       "foreign_key": {
--         "name": "FK_referenced_id_1-ReferencedTableWithPK_id",
--         "on_delete": "NoAction",
--         "on_update": "NoAction",
--         "referenced_column_names": ["id"],
--         "referenced_table_name": "ReferencedTableWithPK",
--         "referencing_column_names": ["referenced_id_1"]
--       },
--       "table_name": "ReferencingWithTwoFK"
--     }
--   ]
-- }

-- @name: Dropping table with cascade should drop both table and constraint
DROP TABLE ReferencedTableWithPK CASCADE;
-- @expect: payload DropTable
-- @json: 1

-- @name: Should create self referencing table
CREATE TABLE SelfReferencingTable (
    id INTEGER PRIMARY KEY,
    name TEXT,
    referenced_id INTEGER,
    FOREIGN KEY (referenced_id) REFERENCES SelfReferencingTable (id)
);
-- @expect: payload Create

-- @name: Dropping self referencing table should succeed
DROP TABLE SelfReferencingTable;
-- @expect: payload DropTable
-- @json: 1

-- @name: Should create parent table for composite foreign key tests
CREATE TABLE CompositeParent (
    id INTEGER PRIMARY KEY,
    tag TEXT
);
-- @expect: payload Create

INSERT INTO CompositeParent VALUES (1, 'alpha'), (2, 'beta');
-- @expect: payload Insert
-- @json: 2

-- @name: Composite foreign key should be accepted when a referenced column is the primary key
CREATE TABLE CompositeChild (
    id INTEGER,
    tag TEXT,
    FOREIGN KEY (id, tag) REFERENCES CompositeParent (id, tag)
);
-- @expect: payload Create

-- @name: Composite foreign key should accept a row matching the whole tuple
INSERT INTO CompositeChild VALUES (1, 'alpha');
-- @expect: payload Insert
-- @json: 1

-- @name: Composite foreign key should reject a row whose non-key column does not match
INSERT INTO CompositeChild VALUES (1, 'beta');
-- @expect: error Insert.CannotFindReferencedValue

-- @name: Composite foreign key should reject a row whose key column is absent
INSERT INTO CompositeChild VALUES (9, 'alpha');
-- @expect: error Insert.CannotFindReferencedValue

-- @name: Composite foreign key should allow NULL in the referencing tuple
INSERT INTO CompositeChild VALUES (1, NULL);
-- @expect: payload Insert
-- @json: 1

-- @name: Composite foreign key should reject an UPDATE breaking the tuple
UPDATE CompositeChild SET tag = 'beta' WHERE id = 1 AND tag = 'alpha';
-- @expect: error Update.CannotFindReferencedValue

-- @name: Composite foreign key should allow an UPDATE keeping the tuple valid
UPDATE CompositeChild SET tag = 'alpha' WHERE id = 1 AND tag = 'alpha';
-- @expect: payload Update
-- @json: 1

-- @name: Cannot delete a parent row referenced by a composite foreign key
DELETE FROM CompositeParent WHERE id = 1;
-- @expect: error Delete.ReferencingColumnExists

-- @name: Can delete a parent row no composite child references
DELETE FROM CompositeParent WHERE id = 2;
-- @expect: payload Delete
-- @json: 1

-- @name: Composite foreign key column counts must match
CREATE TABLE MismatchedChild (
    id INTEGER,
    tag TEXT,
    FOREIGN KEY (id, tag) REFERENCES CompositeParent (id)
);
-- @expect: error Translate.ForeignKeyColumnCountMismatch

-- @name: Should create a parent with two non primary key columns
CREATE TABLE TwoPlainColumnsParent (
    id INTEGER PRIMARY KEY,
    a TEXT,
    b TEXT
);
-- @expect: payload Create

-- @name: Composite foreign key must include the referenced primary key
CREATE TABLE NoPkChild (
    a TEXT,
    b TEXT,
    FOREIGN KEY (a, b) REFERENCES TwoPlainColumnsParent (a, b)
);
-- @expect: error Alter.ReferencingNonPKColumn

-- @name: Column count is checked before duplicates
CREATE TABLE CountBeforeDuplicateChild (
    x INTEGER,
    y INTEGER,
    FOREIGN KEY (x, x) REFERENCES CompositeParent (id)
);
-- @expect: error Translate.ForeignKeyColumnCountMismatch

-- @name: Referenced column list must not contain duplicates
CREATE TABLE DuplicateReferencedChild (
    x INTEGER,
    y INTEGER,
    FOREIGN KEY (x, y) REFERENCES CompositeParent (id, id)
);
-- @expect: error Translate.DuplicateForeignKeyColumn

-- @name: Referencing column list must not contain duplicates
CREATE TABLE DuplicateReferencingChild (
    x INTEGER,
    tag TEXT,
    FOREIGN KEY (x, x) REFERENCES CompositeParent (id, tag)
);
-- @expect: error Translate.DuplicateForeignKeyColumn

-- @name: Single column foreign key should still be accepted
CREATE TABLE SingleFkChild (
    id INTEGER,
    FOREIGN KEY (id) REFERENCES CompositeParent (id)
);
-- @expect: payload Create

-- @name: Composite foreign key should work when the primary key is not the first column
CREATE TABLE ReversedOrderChild (
    tag TEXT,
    id INTEGER,
    FOREIGN KEY (tag, id) REFERENCES CompositeParent (tag, id)
);
-- @expect: payload Create

INSERT INTO ReversedOrderChild VALUES ('alpha', 1);
-- @expect: payload Insert
-- @json: 1

-- @name: Reversed order composite foreign key should still reject a broken tuple
INSERT INTO ReversedOrderChild VALUES ('beta', 1);
-- @expect: error Insert.CannotFindReferencedValue

-- @name: Composite foreign key should allow referencing columns named differently
CREATE TABLE RenamedColumnChild (
    k INTEGER,
    t TEXT,
    FOREIGN KEY (k, t) REFERENCES CompositeParent (id, tag)
);
-- @expect: payload Create

INSERT INTO RenamedColumnChild VALUES (1, 'alpha');
-- @expect: payload Insert
-- @json: 1

-- @name: Composite foreign key should reject a broken tuple across renamed columns
INSERT INTO RenamedColumnChild VALUES (1, 'beta');
-- @expect: error Insert.CannotFindReferencedValue

-- @name: Composite foreign key should reject a multi row insert when any row breaks the tuple
CREATE TABLE MultiRowChild (
    id INTEGER,
    tag TEXT,
    FOREIGN KEY (id, tag) REFERENCES CompositeParent (id, tag)
);
-- @expect: payload Create

-- An earlier step deleted parent row 2, so add a second valid parent tuple.
INSERT INTO CompositeParent VALUES (3, 'gamma');
-- @expect: payload Insert
-- @json: 1

INSERT INTO MultiRowChild VALUES (1, 'alpha'), (3, 'gamma');
-- @expect: payload Insert
-- @json: 2

INSERT INTO MultiRowChild VALUES (1, 'alpha'), (3, 'alpha');
-- @expect: error Insert.CannotFindReferencedValue

-- @name: A rejected multi row insert should leave no rows behind
SELECT id, tag FROM MultiRowChild;
-- @expect: count 2

-- @name: Should support two composite foreign keys on one table
CREATE TABLE SecondParent (
    code TEXT PRIMARY KEY,
    note TEXT
);
-- @expect: payload Create

INSERT INTO SecondParent VALUES ('x', 'n1');
-- @expect: payload Insert
-- @json: 1

CREATE TABLE TwoCompositeFkChild (
    id INTEGER,
    tag TEXT,
    code TEXT,
    note TEXT,
    FOREIGN KEY (id, tag) REFERENCES CompositeParent (id, tag),
    FOREIGN KEY (code, note) REFERENCES SecondParent (code, note)
);
-- @expect: payload Create

INSERT INTO TwoCompositeFkChild VALUES (1, 'alpha', 'x', 'n1');
-- @expect: payload Insert
-- @json: 1

-- @name: The second composite foreign key should be enforced independently
INSERT INTO TwoCompositeFkChild VALUES (1, 'alpha', 'x', 'wrong');
-- @expect: error Insert.CannotFindReferencedValue

-- @name: Should support a self referencing composite foreign key
CREATE TABLE SelfCompositeRef (
    id INTEGER PRIMARY KEY,
    tag TEXT,
    parent_id INTEGER,
    parent_tag TEXT,
    FOREIGN KEY (parent_id, parent_tag) REFERENCES SelfCompositeRef (id, tag)
);
-- @expect: payload Create

INSERT INTO SelfCompositeRef VALUES (1, 'a', NULL, NULL);
-- @expect: payload Insert
-- @json: 1

INSERT INTO SelfCompositeRef VALUES (2, 'b', 1, 'a');
-- @expect: payload Insert
-- @json: 1

-- @name: Self referencing composite foreign key should reject a broken tuple
INSERT INTO SelfCompositeRef VALUES (3, 'c', 1, 'wrong');
-- @expect: error Insert.CannotFindReferencedValue
