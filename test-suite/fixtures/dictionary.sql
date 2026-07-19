SHOW VERSION;
-- @expect: payload ShowVariable.Version

SHOW TABLES
-- @expect: payload ShowVariable.Tables
-- @json:
-- []

CREATE TABLE Foo (id INTEGER, name TEXT NULL, type TEXT NULL) COMMENT='this is table comment';
-- @expect: ok

SHOW TABLES
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Foo"
-- ]

CREATE TABLE Zoo (id INTEGER PRIMARY KEY COMMENT 'hello');
-- @expect: ok

CREATE TABLE Bar (id INTEGER UNIQUE, name TEXT NOT NULL DEFAULT 'NONE');
-- @expect: ok

SHOW TABLES
-- @expect: payload ShowVariable.Tables
-- @json:
-- [
--   "Bar",
--   "Foo",
--   "Zoo"
-- ]

SHOW WHATEVER
-- @expect: error Translate.UnsupportedShowVariableKeyword
-- @json: "WHATEVER"

SHOW ME THE CHICKEN
-- @expect: error Translate.UnsupportedShowVariableStatement
-- @json: "SHOW ME THE CHICKEN"

SELECT * FROM GLUE_TABLES
-- @expect:
-- | TABLE_NAME: Str | COMMENT: Str            |
-- | "Bar"           | NULL                    |
-- | "Foo"           | "this is table comment" |
-- | "Zoo"           | NULL                    |

SELECT * FROM GLUE_TABLE_COLUMNS
-- @expect:
-- | TABLE_NAME: Str | COLUMN_NAME: Str | COLUMN_ID: I64 | NULLABLE: Bool | KEY: Str      | DEFAULT: Str | COMMENT: Str |
-- | "Bar"           | "id"             | 1              | true           | "UNIQUE"      | NULL         | NULL         |
-- | "Bar"           | "name"           | 2              | false          | NULL          | "'NONE'"     | NULL         |
-- | "Foo"           | "id"             | 1              | true           | NULL          | NULL         | NULL         |
-- | "Foo"           | "name"           | 2              | true           | NULL          | NULL         | NULL         |
-- | "Foo"           | "type"           | 3              | true           | NULL          | NULL         | NULL         |
-- | "Zoo"           | "id"             | 1              | false          | "PRIMARY KEY" | NULL         | "hello"      |
