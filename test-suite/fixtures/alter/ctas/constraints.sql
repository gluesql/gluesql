-- @name: Every initial row is checked against explicit NOT NULL
CREATE TABLE Required (id INT NOT NULL) AS VALUES (NULL);
-- @expect: error Value.NullValueOnNotNullField

-- @name: Initial primary keys must be unique within the source
CREATE TABLE DuplicateKeys (id INT PRIMARY KEY) AS VALUES (1), (1);
-- @expect: error Validate.DuplicateEntryOnUniqueField

CREATE TABLE DuplicateValues (id INT, name TEXT UNIQUE) AS VALUES (1, 'same'), (2, 'same');
-- @expect: error Validate.DuplicateEntryOnUniqueField

-- @name: NULLs remain distinct for a nullable UNIQUE column
CREATE TABLE NullableUnique (name TEXT UNIQUE) AS VALUES (NULL), (NULL);
-- @expect: ok

SELECT COUNT(*) AS count FROM NullableUnique;
-- @expect:
-- | count: I64 |
-- | ---------- |
-- | 2          |

CREATE TABLE Parent (id INT PRIMARY KEY);
-- @expect: ok

INSERT INTO Parent VALUES (10);
-- @expect: ok

-- @name: CTAS checks references to an existing table and stores primary keys
CREATE TABLE Child (
    id INT PRIMARY KEY,
    parent INT,
    FOREIGN KEY (parent) REFERENCES Parent(id)
) AS VALUES (1, 10), (2, NULL);
-- @expect: ok

SELECT * FROM Child WHERE id = 1;
-- @expect:
-- | id: I64 | parent: I64 |
-- | ------- | ----------- |
-- | 1       | 10          |

CREATE TABLE MissingParent (
    parent INT,
    FOREIGN KEY (parent) REFERENCES Parent(id)
) AS VALUES (11);
-- @expect: error Validate.CannotFindReferencedValue

-- @name: Self-referencing CTAS is explicitly unsupported, even for an empty source
CREATE TABLE SelfReference (
    id INT PRIMARY KEY,
    parent INT,
    FOREIGN KEY (parent) REFERENCES SelfReference(id)
) AS SELECT id, id FROM Parent WHERE FALSE;
-- @expect: error Planner.UnsupportedSelfReferencingForeignKeyInCtas
-- @json: "SelfReference"

-- @name: Normal CREATE TABLE still accepts a self-reference
CREATE TABLE EmptySelfReference (
    id INT PRIMARY KEY,
    parent INT,
    FOREIGN KEY (parent) REFERENCES EmptySelfReference(id)
);
-- @expect: ok

-- @name: A source evaluation failure occurs before the target schema is stored
CREATE TABLE SourceFailure (id INT) AS SELECT CAST('bad' AS INT);
-- @expect: error Evaluate.TextCastFailed

CREATE TABLE Documents;
-- @expect: ok

INSERT INTO Documents VALUES ('{"id": 1}'), ('{"id": "wrong type"}');
-- @expect: ok

-- @name: Dynamic values must match the declared type in every source row
CREATE TABLE DynamicFailure (id INT) AS SELECT id FROM Documents;
-- @expect: error Value.IncompatibleDataType

-- @name: Value and constraint failures do not create the target table
SELECT COUNT(*) AS count FROM GLUE_TABLES
WHERE TABLE_NAME IN ('Required', 'DuplicateKeys', 'DuplicateValues', 'MissingParent', 'SelfReference', 'SourceFailure', 'DynamicFailure');
-- @expect:
-- | count: I64 |
-- | ---------- |
-- | 0          |
