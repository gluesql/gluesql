CREATE TABLE AddCol (id INTEGER);
-- expect: ok

INSERT INTO AddCol VALUES (1);
-- expect: ok

BEGIN;
-- expect: ok

ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;
-- expect: ok

SELECT * FROM AddCol
-- expect:
-- | id: I64 | new_col: I64 |
-- | 1       | 3            |

ROLLBACK;
-- expect: ok

SELECT * FROM AddCol
-- expect:
-- | id: I64 |
-- | 1       |

BEGIN;
-- expect: ok

ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;
-- expect: ok

COMMIT;
-- expect: ok

SELECT * FROM AddCol
-- expect:
-- | id: I64 | new_col: I64 |
-- | 1       | 3            |
