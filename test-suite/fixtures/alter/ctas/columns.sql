CREATE TABLE Source (
    id INT PRIMARY KEY,
    note TEXT NOT NULL DEFAULT 'new' COMMENT 'source note'
);
-- @expect: ok

INSERT INTO Source VALUES (1, 'one'), (2, 'two');
-- @expect: ok

-- @name: A whole-table wildcard preserves column definitions and primary keys
CREATE TABLE Copied AS SELECT s.* FROM Source s ORDER BY id;
-- @expect: payload Create

SELECT note FROM Copied WHERE id = 1;
-- @expect:
-- | note: Str |
-- | --------- |
-- | "one"     |

-- @name: A copied default remains available to later INSERT
INSERT INTO Copied (id) VALUES (3);
-- @expect: ok

SELECT note FROM Copied WHERE id = 3;
-- @expect:
-- | note: Str |
-- | --------- |
-- | "new"     |

INSERT INTO Copied VALUES (1, 'duplicate');
-- @expect: error Validate.DuplicateEntryOnPrimaryKeyField

INSERT INTO Copied VALUES (4, NULL);
-- @expect: error Value.NullValueOnNotNullField

SELECT COMMENT FROM GLUE_TABLE_COLUMNS
WHERE TABLE_NAME = 'Copied' AND COLUMN_NAME = 'note';
-- @expect:
-- | COMMENT: Str  |
-- | ------------- |
-- | "source note" |

-- @name: An empty filtered copy still preserves defaults
CREATE TABLE EmptyCopy AS SELECT * FROM Source WHERE FALSE;
-- @expect: ok

INSERT INTO EmptyCopy (id) VALUES (5);
-- @expect: ok

SELECT * FROM EmptyCopy WHERE id = 5;
-- @expect:
-- | id: I64 | note: Str |
-- | ------- | --------- |
-- | 5       | "new"     |

-- @name: Ordinary output columns are nullable and do not inherit uniqueness
CREATE TABLE Projected AS SELECT id, note FROM Source;
-- @expect: ok

INSERT INTO Projected VALUES (1, NULL), (NULL, NULL);
-- @expect: payload Insert
-- @json: 2

-- @name: A derived wildcard does not copy physical-table constraints
CREATE TABLE DerivedCopy AS SELECT * FROM (SELECT * FROM Source) d;
-- @expect: ok

INSERT INTO DerivedCopy VALUES (NULL, NULL);
-- @expect: ok

-- @name: Whole-source column aliases preserve definitions under the new names
CREATE TABLE Renamed AS SELECT s.* FROM Source s(new_id, new_note);
-- @expect: ok

INSERT INTO Renamed (new_id) VALUES (6);
-- @expect: ok

SELECT new_note FROM Renamed WHERE new_id = 6;
-- @expect:
-- | new_note: Str |
-- | ------------- |
-- | "new"         |

-- @name: Explicit definitions set names and defaults independently of source labels
CREATE TABLE Explicit (
    target_id INT PRIMARY KEY,
    target_note TEXT DEFAULT 'explicit'
) AS SELECT id, note FROM Source;
-- @expect: ok

INSERT INTO Explicit (target_id) VALUES (7);
-- @expect: ok

SELECT * FROM Explicit WHERE target_id = 7;
-- @expect:
-- | target_id: I64 | target_note: Str |
-- | -------------- | ---------------- |
-- | 7              | "explicit"       |

-- @name: Missing VALUES columns are not filled from defaults
CREATE TABLE MissingValues (id INT, note TEXT DEFAULT 'new') AS VALUES (1);
-- @expect: error Planner.CtasColumnCountMismatch
-- @json: {"expected":2,"found":1}

-- @name: Even an empty SELECT must have the full target width
CREATE TABLE MissingSelect (id INT, note TEXT DEFAULT 'new')
AS SELECT id FROM Source WHERE FALSE;
-- @expect: error Planner.CtasColumnCountMismatch
-- @json: {"expected":2,"found":1}

CREATE TABLE Excess (id INT) AS SELECT id, note FROM Source;
-- @expect: error Planner.CtasColumnCountMismatch
-- @json: {"expected":1,"found":2}

-- @name: IF NOT EXISTS still checks the source during planning
CREATE TABLE IF NOT EXISTS Copied AS SELECT * FROM MissingSource;
-- @expect: error Planner.CtasSourceTableNotFound
-- @json: "MissingSource"

CREATE TABLE IF NOT EXISTS Copied AS SELECT NULL AS unknown;
-- @expect: error Planner.CtasColumnTypeUnknown
-- @json: "unknown"

-- @name: A valid plan for an existing target does not execute its source
CREATE TABLE IF NOT EXISTS Copied AS SELECT CAST('not an integer' AS INT) AS id;
-- @expect: payload Create

SELECT COUNT(*) AS count FROM Copied;
-- @expect:
-- | count: I64 |
-- | ---------- |
-- | 3          |
