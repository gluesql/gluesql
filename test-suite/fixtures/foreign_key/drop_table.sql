CREATE TABLE ReferencedTableWithPK (
    id INTEGER PRIMARY KEY,
    name TEXT
);
-- @expect: ok

INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1');
-- @expect: ok

CREATE TABLE ReferencedTableWithPK_2 (
    id INTEGER PRIMARY KEY,
    name TEXT
);
-- @expect: ok

INSERT INTO ReferencedTableWithPK_2 VALUES (1, 'referenced_table2');
-- @expect: ok

CREATE TABLE ReferencingTable (
    id INT,
    name TEXT,
    referenced_id INT,
    CONSTRAINT MyFkConstraint
        FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        ON DELETE NO ACTION ON UPDATE RESTRICT
);
-- @expect: ok

INSERT INTO ReferencingTable VALUES (1, 'Null is independent', NULL);
-- @expect: ok

CREATE TABLE ReferencingWithTwoFK (
    id INTEGER PRIMARY KEY,
    name TEXT,
    referenced_id_1 INTEGER,
    referenced_id_2 INTEGER,
    FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id),
    FOREIGN KEY (referenced_id_2) REFERENCES ReferencedTableWithPK_2 (id)
);
-- @expect: ok

INSERT INTO ReferencingWithTwoFK
VALUES (1, 'referencing_table with two referenced_table', 1, 1);
-- @expect: ok

-- @name: DROP TABLE lists the foreign keys that prevent deletion
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
--         "referenced_column_name": "id",
--         "referenced_table_name": "ReferencedTableWithPK",
--         "referencing_column_name": "referenced_id"
--       },
--       "table_name": "ReferencingTable"
--     },
--     {
--       "foreign_key": {
--         "name": "FK_referenced_id_1-ReferencedTableWithPK_id",
--         "on_delete": "NoAction",
--         "on_update": "NoAction",
--         "referenced_column_name": "id",
--         "referenced_table_name": "ReferencedTableWithPK",
--         "referencing_column_name": "referenced_id_1"
--       },
--       "table_name": "ReferencingWithTwoFK"
--     }
--   ]
-- }

-- @name: DROP TABLE CASCADE removes the parent table and its incoming foreign keys
DROP TABLE ReferencedTableWithPK CASCADE;
-- @expect: payload DropTable
-- @json: 1

CREATE TABLE SelfReferencingTable (
    id INTEGER PRIMARY KEY,
    name TEXT,
    referenced_id INTEGER,
    FOREIGN KEY (referenced_id) REFERENCES SelfReferencingTable (id)
);
-- @expect: ok

-- @name: A self-referencing table can be dropped without CASCADE
DROP TABLE SelfReferencingTable;
-- @expect: payload DropTable
-- @json: 1

CREATE TABLE CascadeP1 (id INT PRIMARY KEY);
-- @expect: ok

CREATE TABLE CascadeP2 (id INT PRIMARY KEY);
-- @expect: ok

CREATE TABLE CascadeChild (
    a INT,
    b INT,
    c INT,
    FOREIGN KEY (a) REFERENCES CascadeP1 (id),
    FOREIGN KEY (b) REFERENCES CascadeP1 (id),
    FOREIGN KEY (c) REFERENCES CascadeP2 (id)
);
-- @expect: ok

-- @name: CASCADE can drop both parents of a child with multiple foreign keys
DROP TABLE CascadeP1, CascadeP2 CASCADE;
-- @expect: payload DropTable
-- @json: 2

-- @name: The surviving child accepts values for all three removed foreign keys
INSERT INTO CascadeChild VALUES (11, 22, 33);
-- @expect: payload Insert
-- @json: 1

SELECT * FROM CascadeChild;
-- @expect:
-- | a: I64 | b: I64 | c: I64 |
-- | ------ | ------ | ------ |
-- | 11     | 22     | 33     |

CREATE TABLE OrderedParent (id INT PRIMARY KEY);
-- @expect: ok

CREATE TABLE OrderedChild (
    id INT,
    FOREIGN KEY (id) REFERENCES OrderedParent (id)
);
-- @expect: ok

-- @name: CASCADE can drop the child before its parent
DROP TABLE OrderedChild, OrderedParent CASCADE;
-- @expect: payload DropTable
-- @json: 2

-- @name: The child remains absent after its parent is dropped
SELECT * FROM OrderedChild;
-- @expect: error Fetch.TableNotFound
-- @json: "OrderedChild"
