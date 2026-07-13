CREATE TABLE CreateTable1 (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)
-- expect: payload Create

CREATE TABLE CreateTable1 (
    id INTEGER NULL,
    num INTEGER,
    name TEXT COMMENT 'this is comment for name column'
)
-- expect: error Alter.TableAlreadyExists
-- "CreateTable1"

CREATE TABLE IF NOT EXISTS CreateTable2 (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)
-- expect: payload Create

CREATE TABLE IF NOT EXISTS CreateTable2 (
    id2 INTEGER NULL
)
-- expect: payload Create

INSERT INTO CreateTable2 VALUES (NULL, 1, '1');
-- expect: payload Insert
-- 1

INSERT INTO CreateTable2 VALUES (2, 2, '2');
-- expect: payload Insert
-- 1

CREATE TABLE Gluery (id SOMEWHAT);
-- expect: error Translate.UnsupportedDataType
-- "SOMEWHAT"

CREATE TABLE Gluery (id GLOBE);
-- expect: error Translate.UnsupportedDataType
-- "GLOBE"

CREATE TABLE Gluery (id INTEGER CHECK (true));
-- expect: error Translate.UnsupportedColumnOption
-- "CHECK (true)"

CREATE TABLE CreateTable3 (
    id INTEGER,
    ratio FLOAT UNIQUE
)
-- expect: error Alter.UnsupportedDataTypeForUniqueColumn
-- [
--   "ratio",
--   "Float"
-- ]

CREATE TABLE CreateTableFloat32 (
    id INTEGER,
    ratio FLOAT32 PRIMARY KEY
)
-- expect: error Alter.UnsupportedDataTypeForUniqueColumn
-- [
--   "ratio",
--   "Float32"
-- ]

CREATE TABLE Gluery (id BOOLEAN DEFAULT 1 IN (SELECT id FROM Wow))
-- expect: error Evaluate.InSubqueryNotAllowedInStatelessExpr

CREATE TABLE TargetTable AS SELECT * FROM CreateTable2 WHERE 1 = 0
-- expect: payload Create

CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2
-- expect: payload Create

CREATE TABLE TargetTableWithAggregate AS SELECT COUNT(*) FROM CreateTable2
-- expect: payload Create

SELECT * FROM TargetTableWithData
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | NULL    | 1        | "1"       |
-- | 2       | 2        | "2"       |

SELECT * FROM TargetTableWithAggregate
-- expect:
-- | COUNT(*): I64 |
-- | 2             |

CREATE TABLE TargetTableWithLimit AS SELECT * FROM CreateTable2 LIMIT 1
-- expect: payload Create

SELECT * FROM TargetTableWithLimit
-- expect:
-- | id   | num: I64 | name: Str |
-- | NULL | 1        | "1"       |

CREATE TABLE TargetTableWithOffset AS SELECT * FROM CreateTable2 OFFSET 1
-- expect: payload Create

SELECT * FROM TargetTableWithOffset
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 2       | 2        | "2"       |

CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2
-- expect: error Alter.TableAlreadyExists
-- "TargetTableWithData"

CREATE TABLE TargetTableWithData2 AS SELECT * FROM NonExistentTable
-- expect: error Alter.CtasSourceTableNotFound
-- "NonExistentTable"

CREATE TABLE DuplicateColumns (id INT, id INT)
-- expect: error Alter.DuplicateColumnName
-- "id"

CREATE TABLE EmptySource (id INTEGER)
-- expect: payload Create

CREATE TABLE TargetTableWithEmptyAggregate AS SELECT COUNT(*) FROM EmptySource
-- expect: payload Create

SELECT * FROM TargetTableWithEmptyAggregate
-- expect:
-- | COUNT(*): I64 |
-- | 0             |
