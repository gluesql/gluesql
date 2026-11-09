CREATE TABLE TxTest (
    id INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO TxTest VALUES
    (1, 'Friday'),
    (2, 'Phone'),
    (3, 'Vienna');
-- @expect: ok

BEGIN;
-- @expect: payload StartTransaction

DELETE FROM TxTest WHERE id = 3;
-- @expect: payload Delete
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

-- @name: ROLLBACK restores the deleted row
ROLLBACK;
-- @expect: payload Rollback

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |
-- | 3       | "Vienna"  |

BEGIN;
-- @expect: payload StartTransaction

DELETE FROM TxTest WHERE id = 3;
-- @expect: payload Delete
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

-- @name: COMMIT keeps the row deleted
COMMIT;
-- @expect: payload Commit

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

BEGIN;
-- @expect: payload StartTransaction

UPDATE TxTest SET name = 'Sunday' WHERE id = 1;
-- @expect: payload Update
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Sunday"  |
-- | 2       | "Phone"   |

-- @name: ROLLBACK restores the value before UPDATE
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

UPDATE TxTest SET name = 'Sunday' WHERE id = 1;
-- @expect: payload Update
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Sunday"  |
-- | 2       | "Phone"   |

-- @name: COMMIT keeps the updated value
COMMIT;
-- @expect: payload Commit

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Sunday"  |
-- | 2       | "Phone"   |
