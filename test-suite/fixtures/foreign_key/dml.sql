CREATE TABLE ReferencedTableWithPK (
    id INTEGER PRIMARY KEY,
    name TEXT
);
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

-- @name: INSERT rejects a missing referenced value
INSERT INTO ReferencingTable VALUES (1, 'orphan', 1);
-- @expect: error Insert.CannotFindReferencedValue
-- @json:
-- {
--   "column_name": "id",
--   "referenced_value": "1",
--   "table_name": "ReferencedTableWithPK"
-- }

-- @name: INSERT accepts NULL without a matching parent row
INSERT INTO ReferencingTable VALUES (1, 'Null is independent', NULL);
-- @expect: payload Insert
-- @json: 1

INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1');
-- @expect: ok

-- @name: INSERT accepts an existing referenced value
INSERT INTO ReferencingTable VALUES (2, 'referencing_table with referenced_table', 1);
-- @expect: payload Insert
-- @json: 1

-- @name: UPDATE rejects a missing referenced value
UPDATE ReferencingTable SET referenced_id = 2 WHERE id = 2;
-- @expect: error Update.CannotFindReferencedValue
-- @json:
-- {
--   "column_name": "id",
--   "referenced_value": "2",
--   "table_name": "ReferencedTableWithPK"
-- }

-- @name: UPDATE can clear a foreign key to NULL
UPDATE ReferencingTable SET referenced_id = NULL WHERE id = 2;
-- @expect: payload Update
-- @json: 1

-- @name: UPDATE accepts an existing referenced value
UPDATE ReferencingTable SET referenced_id = 1 WHERE id = 2;
-- @expect: payload Update
-- @json: 1

INSERT INTO ReferencedTableWithPK VALUES (2, 'unreferenced row');
-- @expect: ok

-- @name: DELETE rejects a parent row that is still referenced
DELETE FROM ReferencedTableWithPK WHERE id = 1;
-- @expect: error Delete.ReferencingColumnExists
-- @json: "ReferencingTable.referenced_id"

-- @name: DELETE accepts an unreferenced parent row even when the child table has rows
DELETE FROM ReferencedTableWithPK WHERE id = 2;
-- @expect: payload Delete
-- @json: 1

-- @name: DELETE can remove a child row while its parent remains
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

CREATE TABLE ReferencingWithTwoFK (
    id INTEGER PRIMARY KEY,
    name TEXT,
    referenced_id_1 INTEGER,
    referenced_id_2 INTEGER,
    FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id),
    FOREIGN KEY (referenced_id_2) REFERENCES ReferencedTableWithPK_2 (id)
);
-- @expect: payload Create

-- @name: INSERT accepts values that satisfy both foreign keys
INSERT INTO ReferencingWithTwoFK
VALUES (1, 'referencing_table with two referenced_table', 1, 1);
-- @expect: ok

-- @name: UPDATE rejects a missing value for the second foreign key
UPDATE ReferencingWithTwoFK SET referenced_id_2 = 9 WHERE id = 1;
-- @expect: error Update.CannotFindReferencedValue
-- @json:
-- {
--   "column_name": "id",
--   "referenced_value": "9",
--   "table_name": "ReferencedTableWithPK_2"
-- }

CREATE TABLE Employee (
    id INT PRIMARY KEY,
    manager_id INT,
    FOREIGN KEY (manager_id) REFERENCES Employee (id)
);
-- @expect: ok

-- @name: INSERT accepts a self-referencing value created earlier in the same statement
INSERT INTO Employee (id, manager_id) VALUES (1, NULL), (2, 1);
-- @expect: payload Insert
-- @json: 2

-- @name: INSERT accepts a self-referencing value created later in the same statement
INSERT INTO Employee (id, manager_id) VALUES (4, 3), (3, NULL);
-- @expect: payload Insert
-- @json: 2

-- @name: INSERT accepts a row referencing itself
INSERT INTO Employee (id, manager_id) VALUES (5, 5);
-- @expect: payload Insert
-- @json: 1

-- @name: INSERT accepts a self-referencing value from an earlier statement
INSERT INTO Employee (id, manager_id) VALUES (6, 1);
-- @expect: payload Insert
-- @json: 1

-- @name: INSERT rejects a self-referencing value missing from both storage and the statement
INSERT INTO Employee (id, manager_id) VALUES (7, NULL), (8, 999);
-- @expect: error Insert.CannotFindReferencedValue
-- @json:
-- {
--   "column_name": "id",
--   "referenced_value": "999",
--   "table_name": "Employee"
-- }

SELECT id, manager_id FROM Employee ORDER BY id;
-- @expect:
-- | id: I64 | manager_id: I64 |
-- | ------- | --------------- |
-- | 1       | NULL            |
-- | 2       | 1               |
-- | 3       | NULL            |
-- | 4       | 3               |
-- | 5       | 5               |
-- | 6       | 1               |
