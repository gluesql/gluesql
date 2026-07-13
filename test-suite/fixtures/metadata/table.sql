CREATE TABLE Meta (id INT, name TEXT)

-- expect: payload Create

SELECT OBJECT_NAME, OBJECT_TYPE
    FROM GLUE_OBJECTS
    WHERE CREATED > NOW() - INTERVAL 1 MINUTE

-- expect:
-- | OBJECT_NAME: Str | OBJECT_TYPE: Str |
-- | "Meta"           | "TABLE"          |

DROP TABLE Meta

-- expect: payload DropTable
-- 1

SELECT COUNT(*)
    FROM GLUE_OBJECTS
    WHERE CREATED > NOW() - INTERVAL 1 MINUTE

-- expect:
-- | COUNT(*): I64 |
-- | 0             |
