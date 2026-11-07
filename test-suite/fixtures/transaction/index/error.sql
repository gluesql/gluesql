-- A failed statement keeps the explicit transaction active until ROLLBACK.
CREATE TABLE Items (n INTEGER);
-- @expect: ok

INSERT INTO Items VALUES (10);
-- @expect: ok

CREATE INDEX by_n ON Items (n);
-- @expect: ok

BEGIN;
-- @expect: ok

UPDATE Items SET n = 20;
-- @expect: ok

CREATE INDEX by_n ON Items (n);
-- @expect: error Index.IndexNameAlreadyExists
-- @json: "by_n"

SELECT n FROM Items WHERE n = 20;
-- @expect-index: by_n = 20
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 20     |

ROLLBACK;
-- @expect: ok

SELECT n FROM Items WHERE n = 20;
-- @expect-index: by_n = 20
-- @expect:
-- | n: I64 |
-- | ------ |

SELECT n FROM Items WHERE n = 10;
-- @expect-index: by_n = 10
-- @expect:
-- | n: I64 |
-- | ------ |
-- | 10     |
