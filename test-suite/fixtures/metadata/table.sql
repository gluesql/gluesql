CREATE TABLE Meta (id INT, name TEXT)
-- @expect: payload Create

SELECT * FROM GLUE_OBJECTS WHERE FALSE
-- @expect:
-- | OBJECT_NAME | OBJECT_TYPE | CREATED |
-- | ----------- | ----------- | ------- |

SELECT OBJECT_NAME, OBJECT_TYPE
FROM GLUE_OBJECTS
WHERE CREATED > NOW() - INTERVAL 1 MINUTE
-- @expect:
-- | OBJECT_NAME: Str | OBJECT_TYPE: Str |
-- | ---------------- | ---------------- |
-- | "Meta"           | "TABLE"          |

DROP TABLE Meta
-- @expect: payload DropTable
-- @json: 1

SELECT COUNT(*)
FROM GLUE_OBJECTS
WHERE CREATED > NOW() - INTERVAL 1 MINUTE
-- @expect:
-- | COUNT(*): I64 |
-- | ------------- |
-- | 0             |
