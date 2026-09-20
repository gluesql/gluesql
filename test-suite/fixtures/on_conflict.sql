CREATE TABLE Stock (
    id INTEGER PRIMARY KEY,
    sku TEXT UNIQUE,
    quantity INTEGER,
    note TEXT
);
-- @expect: ok

INSERT INTO Stock VALUES (1, 'apple', 10, 'first'), (2, 'pear', 20, 'first');
-- @expect: payload Insert
-- @json: 2

-- @name: DO NOTHING leaves the row that is already there
INSERT INTO Stock VALUES (1, 'apple', 99, 'ignored') ON CONFLICT DO NOTHING
-- @expect: payload Insert
-- @json: 0

SELECT id, sku, quantity, note FROM Stock
-- @expect:
-- | id: I64 | sku: Str | quantity: I64 | note: Str |
-- | ------- | -------- | ------------- | --------- |
-- | 1       | "apple"  | 10            | "first"   |
-- | 2       | "pear"   | 20            | "first"   |

-- @name: a conflict target names the constraint to watch
INSERT INTO Stock VALUES (3, 'plum', 30, 'third') ON CONFLICT (id) DO NOTHING
-- @expect: payload Insert
-- @json: 1

-- @name: DO UPDATE writes the row that is already there, seeing the proposed row as excluded
INSERT INTO Stock VALUES (1, 'apple', 5, 'restock')
ON CONFLICT (id) DO UPDATE SET quantity = quantity + excluded.quantity, note = excluded.note
-- @expect: payload Insert
-- @json: 1

SELECT id, quantity, note FROM Stock WHERE id = 1
-- @expect:
-- | id: I64 | quantity: I64 | note: Str |
-- | ------- | ------------- | --------- |
-- | 1       | 15            | "restock" |

-- @name: EXCLUDED is the same pseudo-table, spelled as PostgreSQL's manual spells it
INSERT INTO Stock VALUES (1, 'apple', 0, 'shouted')
ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note
-- @expect: payload Insert
-- @json: 1

SELECT note FROM Stock WHERE id = 1
-- @expect:
-- | note: Str |
-- | --------- |
-- | "shouted" |

-- @name: a UNIQUE column is a conflict target as much as the primary key
INSERT INTO Stock VALUES (99, 'pear', 7, 'by sku')
ON CONFLICT (sku) DO UPDATE SET quantity = excluded.quantity
-- @expect: payload Insert
-- @json: 1

SELECT id, sku, quantity FROM Stock WHERE sku = 'pear'
-- @expect:
-- | id: I64 | sku: Str | quantity: I64 |
-- | ------- | -------- | ------------- |
-- | 2       | "pear"   | 7             |

-- @name: DO UPDATE ... WHERE leaves the row alone when the condition is false
INSERT INTO Stock VALUES (1, 'apple', 100, 'too big')
ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE excluded.quantity < 50
-- @expect: payload Insert
-- @json: 0

SELECT quantity FROM Stock WHERE id = 1
-- @expect:
-- | quantity: I64 |
-- | ------------- |
-- | 15            |

-- @name: and writes it when the condition is true
INSERT INTO Stock VALUES (1, 'apple', 40, 'small enough')
ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity WHERE excluded.quantity < 50
-- @expect: payload Insert
-- @json: 1

SELECT quantity FROM Stock WHERE id = 1
-- @expect:
-- | quantity: I64 |
-- | ------------- |
-- | 40            |

-- @name: one statement, some rows new and some conflicting
INSERT INTO Stock VALUES (1, 'apple', 1, 'batch'), (4, 'fig', 4, 'batch'), (2, 'pear', 2, 'batch')
ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity, note = excluded.note
-- @expect: payload Insert
-- @json: 3

SELECT id, quantity, note FROM Stock
-- @expect:
-- | id: I64 | quantity: I64 | note: Str |
-- | ------- | ------------- | --------- |
-- | 1       | 1             | "batch"   |
-- | 2       | 2             | "batch"   |
-- | 3       | 30            | "third"   |
-- | 4       | 4             | "batch"   |

-- @name: DO NOTHING also skips a row conflicting with one taken earlier in the same statement
INSERT INTO Stock VALUES (5, 'kiwi', 1, 'once'), (5, 'kiwi', 2, 'twice') ON CONFLICT DO NOTHING
-- @expect: payload Insert
-- @json: 1

SELECT id, note FROM Stock WHERE id = 5
-- @expect:
-- | id: I64 | note: Str |
-- | ------- | --------- |
-- | 5       | "once"    |

-- @name: DO UPDATE cannot write the same row twice
INSERT INTO Stock VALUES (6, 'date', 1, 'a'), (6, 'date', 2, 'b')
ON CONFLICT (id) DO UPDATE SET note = excluded.note
-- @expect: error Insert.ConflictAffectsRowTwice

-- @name: nor can two proposed rows update one stored row
INSERT INTO Stock VALUES (1, 'apple', 1, 'a'), (1, 'apple', 2, 'b')
ON CONFLICT (id) DO UPDATE SET note = excluded.note
-- @expect: error Insert.ConflictAffectsRowTwice

-- @name: DO UPDATE needs a target, as PostgreSQL requires
INSERT INTO Stock VALUES (1, 'apple', 1, 'a') ON CONFLICT DO UPDATE SET note = 'a'
-- @expect: error Insert.ConflictTargetRequired

-- @name: a target has to be a unique column
INSERT INTO Stock VALUES (7, 'lime', 1, 'a') ON CONFLICT (note) DO NOTHING
-- @expect: error Insert.NoUniqueConstraintForTarget
-- @json: "note"

-- @name: and one of them, since a constraint here covers a single column
INSERT INTO Stock VALUES (8, 'melon', 1, 'a') ON CONFLICT (id, sku) DO NOTHING
-- @expect: error Insert.NoUniqueConstraintForTarget
-- @json: "id, sku"

-- @name: a conflict on a constraint the target does not name is still an error
INSERT INTO Stock VALUES (9, 'apple', 1, 'a') ON CONFLICT (id) DO NOTHING
-- @expect: error Validate.DuplicateEntryOnUniqueField

-- @name: DO UPDATE must not write a unique value another row holds
INSERT INTO Stock VALUES (1, 'apple', 1, 'a') ON CONFLICT (id) DO UPDATE SET sku = 'pear'
-- @expect: error Validate.DuplicateEntryOnUniqueField

-- @name: NULL conflicts with nothing, as in every unique constraint
INSERT INTO Stock VALUES (10, NULL, 1, 'a'), (11, NULL, 2, 'b') ON CONFLICT (sku) DO NOTHING
-- @expect: payload Insert
-- @json: 2

SELECT id FROM Stock WHERE sku IS NULL
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 10      |
-- | 11      |

CREATE TABLE Plain (id INTEGER, name TEXT);
-- @expect: ok

INSERT INTO Plain VALUES (1, 'a');
-- @expect: ok

-- @name: a table with no constraint never conflicts
INSERT INTO Plain VALUES (1, 'b') ON CONFLICT DO NOTHING
-- @expect: payload Insert
-- @json: 1

SELECT id, name FROM Plain
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "a"       |
-- | 1       | "b"       |
