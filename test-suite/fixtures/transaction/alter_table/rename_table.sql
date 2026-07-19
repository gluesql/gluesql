CREATE TABLE RenameTable (id INTEGER);
-- @expect: ok

INSERT INTO RenameTable VALUES (1);
-- @expect: ok

BEGIN;
-- @expect: ok

ALTER TABLE RenameTable RENAME TO NewName;
-- @expect: ok

SELECT * FROM RenameTable
-- @expect: error Fetch.TableNotFound
-- @json: "RenameTable"

SELECT * FROM NewName
-- @expect:
-- | id: I64 |
-- | 1       |

ROLLBACK;
-- @expect: ok

SELECT * FROM NewName
-- @expect: error Fetch.TableNotFound
-- @json: "NewName"

SELECT * FROM RenameTable
-- @expect:
-- | id: I64 |
-- | 1       |
