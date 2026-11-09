CREATE TABLE TxTest (
    id INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO TxTest VALUES
    (1, 'Sunday'),
    (2, 'Phone');
-- @expect: ok

BEGIN;
-- @expect: payload StartTransaction

INSERT INTO TxTest VALUES (99, 'pending');
-- @expect: payload Insert
-- @json: 1

-- @name: A missing INSERT target causes a planning error inside the transaction
INSERT INTO MissingTransactionTarget VALUES (1);
-- @expect: error Planner.InsertTableNotFound
-- @json: "MissingTransactionTarget"

-- @name: Earlier writes remain visible after the planning error
SELECT name FROM TxTest WHERE id = 99;
-- @expect:
-- | name: Str |
-- | --------- |
-- | "pending" |

ROLLBACK;
-- @expect: payload Rollback

-- @name: ROLLBACK still discards the writes made before the error
SELECT name FROM TxTest WHERE id = 99;
-- @expect:
-- | name |
-- | ---- |

-- @name: An automatic planning failure leaves the next statement usable
INSERT INTO MissingTransactionTarget VALUES (1);
-- @expect: error Planner.InsertTableNotFound
-- @json: "MissingTransactionTarget"

SELECT name FROM TxTest WHERE id = 99;
-- @expect:
-- | name |
-- | ---- |
