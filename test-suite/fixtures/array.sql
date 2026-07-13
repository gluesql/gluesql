CREATE TABLE Test (id INTEGER DEFAULT 1,name LIST NOT NULL);
-- expect: ok

-- name: basic insert - single item
INSERT INTO Test (id, name) VALUES (1, ['Seongbin','Bernie']);
-- expect: payload Insert
-- 1

-- name: insert multiple rows
INSERT INTO Test (id, name) VALUES (3,Array['Seongbin','Bernie','Chobobdev']), (2,Array['devgony','Henry']);
-- expect: payload Insert
-- 2

INSERT INTO Test VALUES(5,['Jhon']);
-- expect: payload Insert
-- 1

INSERT INTO Test (name) VALUES (['Jane']);
-- expect: payload Insert
-- 1

INSERT INTO Test (name) VALUES (['GlueSQL']);
-- expect: payload Insert
-- 1

SELECT * FROM Test;
-- expect:
-- | id: I64 | name: List                        |
-- | 1       | ["Seongbin","Bernie"]             |
-- | 3       | ["Seongbin","Bernie","Chobobdev"] |
-- | 2       | ["devgony","Henry"]               |
-- | 5       | ["Jhon"]                          |
-- | 1       | ["Jane"]                          |
-- | 1       | ["GlueSQL"]                       |

SELECT ['name', 1, True] AS list;
-- expect:
-- | list: List      |
-- | ["name",1,true] |

SELECT ['GlueSQL', 1, True] [0] AS list;
-- expect:
-- | list: Str |
-- | "GlueSQL" |
