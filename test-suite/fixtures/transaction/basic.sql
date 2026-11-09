CREATE TABLE TxTest (
    id INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO TxTest VALUES
    (1, 'Friday'),
    (2, 'Phone');
-- @expect: ok

BEGIN;
-- @expect: payload StartTransaction

INSERT INTO TxTest VALUES (3, 'New one');
-- @expect: payload Insert
-- @json: 1

-- @name: ROLLBACK discards the inserted row
ROLLBACK;
-- @expect: payload Rollback

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

BEGIN;
-- @expect: payload StartTransaction

INSERT INTO TxTest VALUES (3, 'Vienna');
-- @expect: payload Insert
-- @json: 1

-- @name: The inserted row is visible before COMMIT
SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |
-- | 3       | "Vienna"  |

-- @name: COMMIT keeps the inserted row
COMMIT;
-- @expect: payload Commit

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |
-- | 3       | "Vienna"  |

BEGIN;
-- @expect: ok

SELECT * FROM TxTest;
-- @expect: ok

-- @name: A read-only transaction can be rolled back
ROLLBACK;
-- @expect: ok

BEGIN;
-- @expect: ok

SELECT * FROM TxTest;
-- @expect: ok

-- @name: A read-only transaction can be committed
COMMIT;
-- @expect: ok

BEGIN;
-- @expect: ok

-- @name: An empty transaction can be committed
COMMIT;
-- @expect: ok
