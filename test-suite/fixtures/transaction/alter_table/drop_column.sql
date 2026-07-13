CREATE TABLE DropCol (id INTEGER, num INTEGER);

-- expect: ok

INSERT INTO DropCol VALUES (1, 2);

-- expect: ok

BEGIN;

-- expect: ok

ALTER TABLE DropCol DROP COLUMN num;

-- expect: ok

SELECT * FROM DropCol

-- expect:
-- | id: I64 |
-- | 1       |

ROLLBACK;

-- expect: ok

SELECT * FROM DropCol

-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 2        |

BEGIN;

-- expect: ok

ALTER TABLE DropCol DROP COLUMN num;

-- expect: ok

COMMIT;

-- expect: ok

SELECT * FROM DropCol

-- expect:
-- | id: I64 |
-- | 1       |
