CREATE TABLE Item (
    id INTEGER PRIMARY KEY,
    name TEXT,
    qty INTEGER
);
-- @expect: ok

INSERT INTO Item VALUES (1, 'pen', 3), (2, 'ink', 5);
-- @expect: payload Insert
-- @json: 2

-- @name: DO NOTHING skips rows that conflict and inserts the rest
INSERT INTO Item VALUES (1, 'dup', 9), (3, 'pad', 7) ON CONFLICT DO NOTHING;
-- @expect: payload Insert
-- @json: 1

SELECT * FROM Item ORDER BY id;
-- @expect:
-- | id: I64 | name: Str | qty: I64 |
-- | ------- | --------- | -------- |
-- | 1       | "pen"     | 3        |
-- | 2       | "ink"     | 5        |
-- | 3       | "pad"     | 7        |

-- @name: DO NOTHING with a conflict target
INSERT INTO Item VALUES (2, 'dup', 0) ON CONFLICT (id) DO NOTHING;
-- @expect: payload Insert
-- @json: 0

SELECT qty FROM Item WHERE id = 2;
-- @expect:
-- | qty: I64 |
-- | -------- |
-- | 5        |

-- @name: DO UPDATE applies the assignments to the existing row
INSERT INTO Item VALUES (1, 'pencil', 8)
ON CONFLICT (id) DO UPDATE SET name = 'updated', qty = 100;
-- @expect: payload Insert
-- @json: 1

SELECT * FROM Item WHERE id = 1;
-- @expect:
-- | id: I64 | name: Str | qty: I64 |
-- | ------- | --------- | -------- |
-- | 1       | "updated" | 100      |

-- @name: DO UPDATE assignments read the stored row and the excluded row
INSERT INTO Item VALUES (2, 'ink-v2', 10)
ON CONFLICT (id) DO UPDATE SET qty = qty + excluded.qty, name = excluded.name;
-- @expect: payload Insert
-- @json: 1

SELECT * FROM Item WHERE id = 2;
-- @expect:
-- | id: I64 | name: Str | qty: I64 |
-- | ------- | --------- | -------- |
-- | 2       | "ink-v2"  | 15       |

-- @name: one statement may both insert and update
INSERT INTO Item VALUES (2, 'ink2', 1), (5, 'clip', 4)
ON CONFLICT (id) DO UPDATE SET qty = excluded.qty;
-- @expect: payload Insert
-- @json: 2

SELECT * FROM Item ORDER BY id;
-- @expect:
-- | id: I64 | name: Str | qty: I64 |
-- | ------- | --------- | -------- |
-- | 1       | "updated" | 100      |
-- | 2       | "ink-v2"  | 1        |
-- | 3       | "pad"     | 7        |
-- | 5       | "clip"    | 4        |

-- @name: a row inserted earlier in the statement conflicts with a later one
CREATE TABLE Acc (id INTEGER PRIMARY KEY, total INTEGER);
-- @expect: ok

INSERT INTO Acc VALUES (1, 10), (1, 32)
ON CONFLICT (id) DO UPDATE SET total = total + excluded.total;
-- @expect: payload Insert
-- @json: 2

SELECT total FROM Acc;
-- @expect:
-- | total: I64 |
-- | ---------- |
-- | 42         |

CREATE TABLE Stock (id INTEGER PRIMARY KEY, qty INTEGER);
-- @expect: ok

INSERT INTO Stock VALUES (1, 10), (2, 200);
-- @expect: payload Insert
-- @json: 2

-- @name: the DO UPDATE WHERE clause leaves unmatched rows untouched
INSERT INTO Stock VALUES (1, 5), (2, 5)
ON CONFLICT (id) DO UPDATE SET qty = qty + excluded.qty WHERE qty < 100;
-- @expect: payload Insert
-- @json: 1

SELECT qty FROM Stock ORDER BY id;
-- @expect:
-- | qty: I64 |
-- | -------- |
-- | 15       |
-- | 200      |

CREATE TABLE Peak (id INTEGER PRIMARY KEY, best INTEGER);
-- @expect: ok

INSERT INTO Peak VALUES (1, 50);
-- @expect: payload Insert
-- @json: 1

-- @name: the DO UPDATE WHERE clause may reference excluded
INSERT INTO Peak VALUES (1, 40)
ON CONFLICT (id) DO UPDATE SET best = excluded.best WHERE excluded.best > best;
-- @expect: payload Insert
-- @json: 0

SELECT best FROM Peak;
-- @expect:
-- | best: I64 |
-- | --------- |
-- | 50        |

INSERT INTO Peak VALUES (1, 90)
ON CONFLICT (id) DO UPDATE SET best = excluded.best WHERE excluded.best > best;
-- @expect: payload Insert
-- @json: 1

SELECT best FROM Peak;
-- @expect:
-- | best: I64 |
-- | --------- |
-- | 90        |

CREATE TABLE Login (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE,
    hits INTEGER
);
-- @expect: ok

INSERT INTO Login VALUES (1, 'a@x.io', 10);
-- @expect: payload Insert
-- @json: 1

-- @name: an omitted conflict target covers every unique column
INSERT INTO Login VALUES (9, 'a@x.io', 0), (2, 'b@x.io', 1) ON CONFLICT DO NOTHING;
-- @expect: payload Insert
-- @json: 1

SELECT id, email FROM Login ORDER BY id;
-- @expect:
-- | id: I64 | email: Str |
-- | ------- | ---------- |
-- | 1       | "a@x.io"   |
-- | 2       | "b@x.io"   |

-- @name: a conflict target may name a UNIQUE column
INSERT INTO Login VALUES (7, 'a@x.io', 1)
ON CONFLICT (email) DO UPDATE SET hits = hits + excluded.hits;
-- @expect: payload Insert
-- @json: 1

SELECT id, hits FROM Login WHERE email = 'a@x.io';
-- @expect:
-- | id: I64 | hits: I64 |
-- | ------- | --------- |
-- | 1       | 11        |

-- @name: a conflict on a unique column outside the target still fails
INSERT INTO Login VALUES (5, 'a@x.io', 0) ON CONFLICT (id) DO NOTHING;
-- @expect: error Validate.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "Str": "a@x.io"
--   },
--   "email"
-- ]

-- @name: DO UPDATE may not introduce a duplicate in another unique column
INSERT INTO Login VALUES (1, 'x', 0)
ON CONFLICT (id) DO UPDATE SET email = 'b@x.io';
-- @expect: error Validate.DuplicateEntryOnUniqueField
-- @json:
-- [
--   {
--     "Str": "b@x.io"
--   },
--   "email"
-- ]

-- @name: a failed statement leaves the table unchanged
SELECT id, email, hits FROM Login ORDER BY id;
-- @expect:
-- | id: I64 | email: Str | hits: I64 |
-- | ------- | ---------- | --------- |
-- | 1       | "a@x.io"   | 11        |
-- | 2       | "b@x.io"   | 1         |

CREATE TABLE Member (
    id INTEGER PRIMARY KEY,
    handle TEXT UNIQUE,
    score INTEGER
);
-- @expect: ok

INSERT INTO Member VALUES (1, 'alice', 1), (2, 'bob', 2);
-- @expect: payload Insert
-- @json: 2

-- @name: DO UPDATE may change a unique column to a value that is not in use
INSERT INTO Member VALUES (1, 'zed', 0)
ON CONFLICT (id) DO UPDATE SET handle = excluded.handle;
-- @expect: payload Insert
-- @json: 1

SELECT id, handle FROM Member ORDER BY id;
-- @expect:
-- | id: I64 | handle: Str |
-- | ------- | ----------- |
-- | 1       | "zed"       |
-- | 2       | "bob"       |

-- @name: the old unique value is freed once it is no longer in use
INSERT INTO Member VALUES (3, 'alice', 5);
-- @expect: payload Insert
-- @json: 1

-- @name: the new unique value is still tracked and guards against duplicates
INSERT INTO Member VALUES (4, 'zed', 9)
ON CONFLICT (handle) DO UPDATE SET score = score + excluded.score;
-- @expect: payload Insert
-- @json: 1

SELECT id, handle, score FROM Member ORDER BY id;
-- @expect:
-- | id: I64 | handle: Str | score: I64 |
-- | ------- | ----------- | ---------- |
-- | 1       | "zed"       | 10         |
-- | 2       | "bob"       | 2          |
-- | 3       | "alice"     | 5          |

CREATE TABLE Ticket (code TEXT UNIQUE, status TEXT);
-- @expect: ok

INSERT INTO Ticket VALUES ('t1', 'open'), ('t2', 'open');
-- @expect: payload Insert
-- @json: 2

-- @name: DO UPDATE against a table without a primary key updates the matching row and inserts the rest
INSERT INTO Ticket VALUES ('t1', 'closed'), ('t3', 'open')
ON CONFLICT (code) DO UPDATE SET status = excluded.status;
-- @expect: payload Insert
-- @json: 2

SELECT code, status FROM Ticket ORDER BY code;
-- @expect:
-- | code: Str | status: Str |
-- | --------- | ----------- |
-- | "t1"      | "closed"    |
-- | "t2"      | "open"      |
-- | "t3"      | "open"      |

CREATE TABLE Tag (id INTEGER PRIMARY KEY, label TEXT UNIQUE);
-- @expect: ok

INSERT INTO Tag VALUES (1, NULL);
-- @expect: payload Insert
-- @json: 1

-- @name: NULL never conflicts
INSERT INTO Tag VALUES (2, NULL) ON CONFLICT DO NOTHING;
-- @expect: payload Insert
-- @json: 1

SELECT id FROM Tag ORDER BY id;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 2       |

CREATE TABLE Plain (a INTEGER, b INTEGER);
-- @expect: ok

INSERT INTO Plain VALUES (1, 1);
-- @expect: payload Insert
-- @json: 1

-- @name: a table without unique columns never conflicts
INSERT INTO Plain VALUES (1, 1), (2, 2) ON CONFLICT DO NOTHING;
-- @expect: payload Insert
-- @json: 2

SELECT a FROM Plain ORDER BY a;
-- @expect:
-- | a: I64 |
-- | ------ |
-- | 1      |
-- | 1      |
-- | 2      |

-- @name: a composite conflict target is rejected
INSERT INTO Item VALUES (1, 'p', 0) ON CONFLICT (id, name) DO NOTHING;
-- @expect: error Insert.ConflictTargetMustBeSingleColumn

-- @name: a conflict target must be a primary key or unique column
INSERT INTO Item VALUES (1, 'p', 0) ON CONFLICT (name) DO NOTHING;
-- @expect: error Insert.ConflictTargetNotUnique
-- @json: "name"

-- @name: MySQL's ON DUPLICATE KEY UPDATE is not supported
INSERT INTO Item VALUES (1, 'p', 0) ON DUPLICATE KEY UPDATE qty = 1;
-- @expect: error Translate.UnsupportedInsertOption
-- @json: "ON CONFLICT clause"

-- @name: ON CONFLICT ON CONSTRAINT is not supported
INSERT INTO Item VALUES (1, 'p', 0) ON CONFLICT ON CONSTRAINT item_pkey DO NOTHING;
-- @expect: error Translate.UnsupportedInsertOption
-- @json: "ON CONFLICT clause"

-- @name: DO UPDATE requires a conflict target
INSERT INTO Item VALUES (1, 'p', 0) ON CONFLICT DO UPDATE SET qty = 1;
-- @expect: error Insert.ConflictTargetRequiredForDoUpdate

-- @name: DO UPDATE may not assign the primary key
INSERT INTO Item VALUES (1, 'p', 0) ON CONFLICT (id) DO UPDATE SET id = 99;
-- @expect: error Update.UpdateOnPrimaryKeyNotSupported
-- @json: "id"

SELECT * FROM Item ORDER BY id;
-- @expect:
-- | id: I64 | name: Str | qty: I64 |
-- | ------- | --------- | -------- |
-- | 1       | "updated" | 100      |
-- | 2       | "ink-v2"  | 1        |
-- | 3       | "pad"     | 7        |
-- | 5       | "clip"    | 4        |

CREATE TABLE Doc;
-- @expect: ok

INSERT INTO Doc VALUES ('{"a": 1}');
-- @expect: payload Insert
-- @json: 1

-- @name: ON CONFLICT requires a schema
INSERT INTO Doc VALUES ('{"a": 2}') ON CONFLICT DO NOTHING;
-- @expect: error Insert.OnConflictOnSchemalessTable
-- @json: "Doc"
