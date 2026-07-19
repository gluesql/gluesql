CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)
-- @expect: ok

INSERT INTO DropTable (id, num, name) VALUES (1, 2, 'Hello')
-- @expect: ok

SELECT id, num, name FROM DropTable;
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

DROP TABLE DropTable;
-- @expect: payload DropTable
-- @json: 1

DROP TABLE DropTable;
-- @expect: error Alter.TableNotFound
-- @json: "DropTable"

CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)
-- @expect: payload Create

DROP TABLE IF EXISTS DropTable;
-- @expect: payload DropTable
-- @json: 1

DROP TABLE IF EXISTS DropTable;
-- @expect: payload DropTable
-- @json: 0

SELECT id, num, name FROM DropTable;
-- @expect: error Fetch.TableNotFound
-- @json: "DropTable"

CREATE TABLE DropTable (
    id INT,
    num INT,
    name TEXT
)
-- @expect: payload Create

SELECT id, num, name FROM DropTable;
-- @expect:
-- | id | num | name |

DROP VIEW DropTable;
-- @expect: error Translate.UnsupportedStatement
-- @json: "DROP VIEW DropTable"

CREATE TABLE DropTable1 (
    id INT,
    num INT,
    name TEXT
)
-- @expect: payload Create

CREATE TABLE DropTable2 (
    id INT,
    num INT,
    name TEXT
)
-- @expect: payload Create

DROP TABLE DropTable1, DropTable2;
-- @expect: payload DropTable
-- @json: 2

SELECT id, num, name FROM DropTable1;
-- @expect: error Fetch.TableNotFound
-- @json: "DropTable1"

SELECT id, num, name FROM DropTable2;
-- @expect: error Fetch.TableNotFound
-- @json: "DropTable2"

CREATE TABLE DropTable1 (
    id INT,
    num INT,
    name TEXT
)
-- @expect: payload Create

CREATE TABLE DropTable2 (
    id INT,
    num INT,
    name TEXT
)
-- @expect: payload Create

DROP TABLE IF EXISTS DropTable1, DropTable2;
-- @expect: payload DropTable
-- @json: 2

SELECT id, num, name FROM DropTable1;
-- @expect: error Fetch.TableNotFound
-- @json: "DropTable1"

SELECT id, num, name FROM DropTable2;
-- @expect: error Fetch.TableNotFound
-- @json: "DropTable2"

CREATE TABLE DropTable1 (
    id INT,
    num INT,
    name TEXT
)
-- @expect: payload Create

DROP TABLE IF EXISTS DropTable1, DropTable2;
-- @expect: payload DropTable
-- @json: 1

SELECT id, num, name FROM DropTable1;
-- @expect: error Fetch.TableNotFound
-- @json: "DropTable1"

SELECT id, num, name FROM DropTable2;
-- @expect: error Fetch.TableNotFound
-- @json: "DropTable2"
