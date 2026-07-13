CREATE TABLE Meta (id INT, name TEXT);
-- expect: payload Create

CREATE INDEX Meta_id ON Meta (id);
-- expect: payload CreateIndex

CREATE INDEX Meta_name ON Meta (name);
-- expect: payload CreateIndex

SELECT OBJECT_NAME, OBJECT_TYPE FROM GLUE_OBJECTS;
-- expect:
-- | OBJECT_NAME: Str | OBJECT_TYPE: Str |
-- | "Meta"           | "TABLE"          |
-- | "Meta_id"        | "INDEX"          |
-- | "Meta_name"      | "INDEX"          |
