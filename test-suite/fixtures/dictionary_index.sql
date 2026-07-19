CREATE TABLE Foo (id INT, name TEXT);
-- @expect: ok

CREATE INDEX Foo_id ON Foo (id);
-- @expect: ok

CREATE INDEX Foo_id_2 ON Foo (id + 2);
-- @expect: ok

SELECT * FROM GLUE_INDEXES;
-- @expect:
-- | TABLE_NAME: Str | INDEX_NAME: Str | ORDER: Str | EXPRESSION: Str | UNIQUENESS: Bool |
-- | "Foo"           | "Foo_id"        | "BOTH"     | "id"            | false            |
-- | "Foo"           | "Foo_id_2"      | "BOTH"     | "id + 2"        | false            |

CREATE TABLE Bar (id INT PRIMARY KEY, name TEXT);
-- @expect: ok

CREATE INDEX Bar_name_concat ON Bar (name + '_');
-- @expect: ok

SELECT * FROM GLUE_INDEXES;
-- @expect:
-- | TABLE_NAME: Str | INDEX_NAME: Str   | ORDER: Str | EXPRESSION: Str | UNIQUENESS: Bool |
-- | "Bar"           | "PRIMARY"         | "BOTH"     | "id"            | true             |
-- | "Bar"           | "Bar_name_concat" | "BOTH"     | "name + '_'"    | false            |
-- | "Foo"           | "Foo_id"          | "BOTH"     | "id"            | false            |
-- | "Foo"           | "Foo_id_2"        | "BOTH"     | "id + 2"        | false            |

DROP INDEX Bar.PRIMARY;
-- @expect: error Translate.CannotDropPrimary

CREATE INDEX Primary ON Foo (id);
-- @expect: error Translate.ReservedIndexName
-- @json: "Primary"
