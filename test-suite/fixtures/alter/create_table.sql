CREATE TABLE CreateTable1 (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)
-- @expect: payload Create

CREATE TABLE CreateTable1 (
    id INTEGER NULL,
    num INTEGER,
    name TEXT COMMENT 'this is comment for name column'
)
-- @expect: error Alter.TableAlreadyExists
-- @json: "CreateTable1"

CREATE TABLE IF NOT EXISTS CreateTable2 (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)
-- @expect: payload Create

CREATE TABLE IF NOT EXISTS CreateTable2 (
    id2 INTEGER NULL
)
-- @expect: payload Create

INSERT INTO CreateTable2 VALUES (NULL, 1, '1');
-- @expect: payload Insert
-- @json: 1

INSERT INTO CreateTable2 VALUES (2, 2, '2');
-- @expect: payload Insert
-- @json: 1

CREATE TABLE Gluery (id SOMEWHAT);
-- @expect: error Translate.UnsupportedDataType
-- @json: "SOMEWHAT"

CREATE TABLE Gluery (id GLOBE);
-- @expect: error Translate.UnsupportedDataType
-- @json: "GLOBE"

CREATE TABLE Gluery (id INTEGER CHECK (true));
-- @expect: error Translate.UnsupportedColumnOption
-- @json: "CHECK (true)"

CREATE TABLE CreateTable3 (
    id INTEGER,
    ratio FLOAT UNIQUE
)
-- @expect: error Alter.UnsupportedDataTypeForUniqueColumn
-- @json:
-- [
--   "ratio",
--   "Float"
-- ]

CREATE TABLE CreateTableFloat32 (
    id INTEGER,
    ratio FLOAT32 PRIMARY KEY
)
-- @expect: error Alter.UnsupportedDataTypeForUniqueColumn
-- @json:
-- [
--   "ratio",
--   "Float32"
-- ]

CREATE TABLE Gluery (id BOOLEAN DEFAULT 1 IN (SELECT id FROM Wow))
-- @expect: error Evaluate.InSubqueryNotAllowedInStatelessExpr

CREATE TABLE TargetTable AS SELECT * FROM CreateTable2 WHERE 1 = 0
-- @expect: payload Create

CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2
-- @expect: payload Create

CREATE TABLE TargetTableWithAggregate AS SELECT COUNT(*) FROM CreateTable2
-- @expect: payload Create

-- @name: CTAS infers schema from a filtered JOIN result
CREATE TABLE TargetTableWithFilteredJoin AS
SELECT A.id AS left_id, B.id AS right_id
FROM CreateTable2 A JOIN CreateTable2 B
WHERE A.id = B.id
-- @expect: payload Create

SELECT * FROM TargetTableWithFilteredJoin
-- @expect:
-- | left_id: I64 | right_id: I64 |
-- | ------------ | ------------- |
-- | 2            | 2             |

SELECT * FROM TargetTableWithData
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | ------- | -------- | --------- |
-- | NULL    | 1        | "1"       |
-- | 2       | 2        | "2"       |

SELECT * FROM TargetTableWithAggregate
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 2             |

CREATE TABLE TargetTableWithLimit AS SELECT * FROM CreateTable2 LIMIT 1
-- @expect: payload Create

SELECT * FROM TargetTableWithLimit
-- @expect:
-- | id   | num: I64 | name: Str |
-- | ---- | -------- | --------- |
-- | NULL | 1        | "1"       |

CREATE TABLE TargetTableWithOffset AS SELECT * FROM CreateTable2 OFFSET 1
-- @expect: payload Create

SELECT * FROM TargetTableWithOffset
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | ------- | -------- | --------- |
-- | 2       | 2        | "2"       |

-- @name: CTAS preserves an ORDER BY terminal source
CREATE TABLE TargetTableWithOrder AS
SELECT * FROM CreateTable2 ORDER BY num DESC
-- @expect: payload Create

SELECT * FROM TargetTableWithOrder
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | ------- | -------- | --------- |
-- | 2       | 2        | "2"       |
-- | NULL    | 1        | "1"       |

-- @name: CTAS composes ORDER BY with OFFSET
CREATE TABLE TargetTableWithOrderOffset AS
SELECT * FROM CreateTable2 ORDER BY num DESC OFFSET 1
-- @expect: payload Create

SELECT * FROM TargetTableWithOrderOffset
-- @expect:
-- | id   | num: I64 | name: Str |
-- | ---- | -------- | --------- |
-- | NULL | 1        | "1"       |

-- @name: CTAS composes ORDER BY with LIMIT
CREATE TABLE TargetTableWithOrderLimit AS
SELECT * FROM CreateTable2 ORDER BY num DESC LIMIT 1
-- @expect: payload Create

SELECT * FROM TargetTableWithOrderLimit
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | ------- | -------- | --------- |
-- | 2       | 2        | "2"       |

-- @name: CTAS preserves a DISTINCT terminal source
CREATE TABLE TargetTableWithDistinct AS
SELECT DISTINCT num FROM CreateTable2
-- @expect: payload Create

SELECT * FROM TargetTableWithDistinct ORDER BY num
-- @expect:
-- | num: I64 |
-- | -------- |
-- | 1        |
-- | 2        |

-- @name: CTAS composes DISTINCT with LIMIT
CREATE TABLE TargetTableWithDistinctLimit AS
SELECT DISTINCT num FROM CreateTable2 WHERE num = 1 LIMIT 1
-- @expect: payload Create

SELECT * FROM TargetTableWithDistinctLimit
-- @expect:
-- | num: I64 |
-- | -------- |
-- | 1        |

-- @name: CTAS preserves the complete SELECT terminal pipeline
CREATE TABLE TargetTableWithPipeline AS
SELECT DISTINCT num
FROM CreateTable2
ORDER BY num DESC
LIMIT 1 OFFSET 1
-- @expect: payload Create

SELECT * FROM TargetTableWithPipeline
-- @expect:
-- | num: I64 |
-- | -------- |
-- | 1        |

CREATE TABLE TargetTableWithData AS SELECT * FROM CreateTable2
-- @expect: error Alter.TableAlreadyExists
-- @json: "TargetTableWithData"

CREATE TABLE TargetTableWithData2 AS SELECT * FROM NonExistentTable
-- @expect: error Alter.CtasSourceTableNotFound
-- @json: "NonExistentTable"

CREATE TABLE DuplicateColumns (id INT, id INT)
-- @expect: error Alter.DuplicateColumnName
-- @json: "id"

CREATE TEMPORARY TABLE TempTable (id INTEGER)
-- @expect: error Translate.UnsupportedCreateTableOption
-- @json: "TEMPORARY clause"

CREATE TABLE LikeTable LIKE CreateTable1
-- @expect: error Translate.UnsupportedCreateTableOption
-- @json: "LIKE clause"

CREATE TABLE CloneTable CLONE CreateTable1
-- @expect: error Translate.UnsupportedCreateTableOption
-- @json: "CLONE clause"

CREATE TABLE EmptySource (id INTEGER)
-- @expect: payload Create

CREATE TABLE TargetTableWithEmptyAggregate AS SELECT COUNT(*) FROM EmptySource
-- @expect: payload Create

SELECT * FROM TargetTableWithEmptyAggregate
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 0             |
