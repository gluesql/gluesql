CREATE TABLE RenameCol (id INTEGER);
-- @expect: ok

INSERT INTO RenameCol VALUES (1);
-- @expect: ok

BEGIN;
-- @expect: ok

ALTER TABLE RenameCol RENAME COLUMN id TO new_id;
-- @expect: ok

SELECT * FROM RenameCol
-- @expect:
-- | new_id: I64 |
-- | ----------- |
-- | 1           |

ROLLBACK;
-- @expect: ok

SELECT * FROM RenameCol
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |

BEGIN;
-- @expect: ok

ALTER TABLE RenameCol RENAME COLUMN id TO new_id;
-- @expect: ok

COMMIT;
-- @expect: ok

SELECT * FROM RenameCol
-- @expect:
-- | new_id: I64 |
-- | ----------- |
-- | 1           |
