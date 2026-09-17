CREATE TABLE Item (
    id INTEGER PRIMARY KEY,
    name TEXT,
    qty INTEGER
);
-- @expect: ok

INSERT INTO Item VALUES (1, 'pen', 3), (2, 'ink', 5);
-- @expect: payload Insert
-- @json: 2

-- @name: INSERT ... RETURNING * returns the stored rows in statement order
INSERT INTO Item VALUES (3, 'pad', 7), (4, 'clip', 1) RETURNING *;
-- @expect:
-- | id: I64 | name: Str | qty: I64 |
-- | ------- | --------- | -------- |
-- | 3       | "pad"     | 7        |
-- | 4       | "clip"    | 1        |

-- @name: RETURNING accepts the projection shapes a SELECT accepts
INSERT INTO Item VALUES (5, 'tape', 7) RETURNING id, qty * 2 AS double, name;
-- @expect:
-- | id: I64 | double: I64 | name: Str |
-- | ------- | ----------- | --------- |
-- | 5       | 14          | "tape"    |

-- @name: RETURNING accepts qualified columns and a qualified star
INSERT INTO Item VALUES (6, 'glue', 2) RETURNING Item.id, Item.*;
-- @expect:
-- | id: I64 | id: I64 | name: Str | qty: I64 |
-- | ------- | ------- | --------- | -------- |
-- | 6       | 6       | "glue"    | 2        |

-- @name: a qualified star must name the target table
INSERT INTO Item VALUES (7, 'nib', 1) RETURNING Other.*;
-- @expect: error Execute.ReturningTableNotFound
-- @json: "Other"

-- @name: DO UPDATE reports the rows it stored, skipping the ones it did not
INSERT INTO Item VALUES (2, 'ink2', 10), (8, 'jar', 2), (1, 'skip', 0)
ON CONFLICT (id) DO UPDATE SET qty = qty + excluded.qty WHERE qty < 4
RETURNING id, qty;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 8       | 2        |
-- | 1       | 3        |

-- @name: DO NOTHING reports only the rows it inserted
INSERT INTO Item VALUES (1, 'dup', 0), (9, 'new', 4) ON CONFLICT DO NOTHING RETURNING id;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 9       |

-- @name: UPDATE ... RETURNING reports the rows after the update
UPDATE Item SET qty = qty * 10 WHERE id = 1 RETURNING id, qty AS scaled;
-- @expect:
-- | id: I64 | scaled: I64 |
-- | ------- | ----------- |
-- | 1       | 30          |

-- @name: an UPDATE that matches nothing returns the labels and no rows
UPDATE Item SET qty = 1 WHERE id = 999 RETURNING id;
-- @expect:
-- | id: I64 |
-- | ------- |

-- @name: DELETE ... RETURNING reports the rows as they were before the delete
DELETE FROM Item WHERE id = 2 RETURNING id, name;
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 2       | "ink"     |

SELECT id FROM Item ORDER BY id;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 3       |
-- | 4       |
-- | 5       |
-- | 6       |
-- | 8       |
-- | 9       |

CREATE TABLE Pair (id INTEGER PRIMARY KEY, v INTEGER);
-- @expect: ok

INSERT INTO Pair VALUES (1, 1), (2, 2);
-- @expect: payload Insert
-- @json: 2

-- @name: UPDATE ... RETURNING * expands to the target table's columns
UPDATE Pair SET v = 0 RETURNING *;
-- @expect:
-- | id: I64 | v: I64 |
-- | ------- | ------ |
-- | 1       | 0      |
-- | 2       | 0      |

-- @name: without RETURNING the statements keep reporting counts
INSERT INTO Pair VALUES (1, 5), (3, 6) ON CONFLICT (id) DO UPDATE SET v = excluded.v;
-- @expect: payload Insert
-- @json: 2

UPDATE Pair SET v = 9 WHERE id = 3;
-- @expect: payload Update
-- @json: 1

DELETE FROM Pair WHERE id = 3;
-- @expect: payload Delete
-- @json: 1

CREATE TABLE Doc;
-- @expect: ok

INSERT INTO Doc VALUES ('{"a": 1}');
-- @expect: payload Insert
-- @json: 1

-- @name: RETURNING requires a schema on INSERT
INSERT INTO Doc VALUES ('{"a": 2}') RETURNING *;
-- @expect: error Execute.ReturningOnSchemalessTable
-- @json: "Doc"

-- @name: RETURNING requires a schema on UPDATE
UPDATE Doc SET a = 2 RETURNING *;
-- @expect: error Execute.ReturningOnSchemalessTable
-- @json: "Doc"

-- @name: RETURNING requires a schema on DELETE
DELETE FROM Doc WHERE TRUE RETURNING *;
-- @expect: error Execute.ReturningOnSchemalessTable
-- @json: "Doc"

CREATE TABLE Guard (id INTEGER PRIMARY KEY, qty INTEGER);
-- @expect: ok

INSERT INTO Guard VALUES (1, 1), (2, 2);
-- @expect: payload Insert
-- @json: 2

CREATE TABLE Many (id INTEGER);
-- @expect: ok

INSERT INTO Many VALUES (1), (2);
-- @expect: payload Insert
-- @json: 2

-- @name: an INSERT whose RETURNING names an unknown column stores nothing
INSERT INTO Guard VALUES (3, 3) RETURNING missing;
-- @expect: error Evaluate.IdentifierNotFound
-- @json: "missing"

-- @name: the failed INSERT left the table unchanged
SELECT id, qty FROM Guard ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 1        |
-- | 2       | 2        |

-- @name: an INSERT whose RETURNING subquery returns several rows stores nothing
INSERT INTO Guard VALUES (3, 3) RETURNING (SELECT id FROM Many) AS one;
-- @expect: error Evaluate.MoreThanOneRowReturned

-- @name: the failed INSERT left the table unchanged
SELECT id, qty FROM Guard ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 1        |
-- | 2       | 2        |

-- @name: a DO UPDATE whose RETURNING fails updates nothing
INSERT INTO Guard VALUES (1, 9) ON CONFLICT (id) DO UPDATE SET qty = excluded.qty
RETURNING (SELECT id FROM Many) AS one;
-- @expect: error Evaluate.MoreThanOneRowReturned

-- @name: the failed DO UPDATE left the table unchanged
SELECT id, qty FROM Guard ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 1        |
-- | 2       | 2        |

-- @name: a DO UPDATE that inserts a fresh row also stores nothing when RETURNING fails
INSERT INTO Guard VALUES (4, 4) ON CONFLICT (id) DO UPDATE SET qty = excluded.qty
RETURNING missing;
-- @expect: error Evaluate.IdentifierNotFound
-- @json: "missing"

-- @name: the failed DO UPDATE inserted no row
SELECT id, qty FROM Guard ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 1        |
-- | 2       | 2        |

-- @name: an UPDATE whose RETURNING subquery returns several rows updates nothing
UPDATE Guard SET qty = 100 WHERE id = 1 RETURNING (SELECT id FROM Many) AS one;
-- @expect: error Evaluate.MoreThanOneRowReturned

-- @name: the failed UPDATE left the table unchanged
SELECT id, qty FROM Guard ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 1        |
-- | 2       | 2        |

-- @name: a DELETE whose RETURNING names an unknown column deletes nothing
DELETE FROM Guard WHERE id = 1 RETURNING missing;
-- @expect: error Evaluate.IdentifierNotFound
-- @json: "missing"

-- @name: the failed DELETE left the table unchanged
SELECT id, qty FROM Guard ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 1        |
-- | 2       | 2        |
