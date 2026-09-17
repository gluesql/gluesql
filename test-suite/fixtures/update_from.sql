CREATE TABLE Item (
    id INTEGER PRIMARY KEY,
    name TEXT,
    qty INTEGER
);
-- @expect: ok

CREATE TABLE Restock (item_id INTEGER, amount INTEGER);
-- @expect: ok

INSERT INTO Item VALUES (1, 'pen', 3), (2, 'ink', 5), (3, 'pad', 7);
-- @expect: payload Insert
-- @json: 3

INSERT INTO Restock VALUES (1, 10), (3, 20);
-- @expect: payload Insert
-- @json: 2

-- @name: UPDATE ... FROM updates every target row that matches a source row
UPDATE Item SET qty = qty + r.amount FROM Restock AS r WHERE id = r.item_id;
-- @expect: payload Update
-- @json: 2

SELECT id, qty FROM Item ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 13       |
-- | 2       | 5        |
-- | 3       | 27       |

-- @name: the source table may be referenced by its own name
UPDATE Item SET qty = Restock.amount FROM Restock WHERE id = Restock.item_id;
-- @expect: payload Update
-- @json: 2

SELECT id, qty FROM Item ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 10       |
-- | 2       | 5        |
-- | 3       | 20       |

-- @name: RETURNING may project the matched source row
UPDATE Item SET qty = qty + r.amount FROM Restock AS r WHERE id = r.item_id
RETURNING id, qty, r.amount AS applied;
-- @expect:
-- | id: I64 | qty: I64 | applied: I64 |
-- | ------- | -------- | ------------ |
-- | 1       | 20       | 10           |
-- | 3       | 40       | 20           |

-- @name: a star in RETURNING still expands to the target table only
UPDATE Item SET qty = qty FROM Restock AS r WHERE id = r.item_id RETURNING *;
-- @expect:
-- | id: I64 | name: Str | qty: I64 |
-- | ------- | --------- | -------- |
-- | 1       | "pen"     | 20       |
-- | 3       | "pad"     | 40       |

-- @name: a target row that matches no source row is left untouched
UPDATE Item SET qty = 0 FROM Restock AS r WHERE id = r.item_id AND r.amount > 999
RETURNING id;
-- @expect:
-- | id: I64 |
-- | ------- |

SELECT id, qty FROM Item ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 20       |
-- | 2       | 5        |
-- | 3       | 40       |

INSERT INTO Restock VALUES (2, 1), (2, 2);
-- @expect: payload Insert
-- @json: 2

-- @name: a target row matching more than one source row fails the statement
UPDATE Item SET qty = r.amount FROM Restock AS r WHERE id = r.item_id;
-- @expect: error Update.MultipleSourceRowsForTargetRow

-- @name: the failed statement left the table unchanged
SELECT id, qty FROM Item ORDER BY id;
-- @expect:
-- | id: I64 | qty: I64 |
-- | ------- | -------- |
-- | 1       | 20       |
-- | 2       | 5        |
-- | 3       | 40       |

CREATE TABLE Score (id INTEGER PRIMARY KEY, points INTEGER);
-- @expect: ok

CREATE TABLE Bonus (id INTEGER, points INTEGER);
-- @expect: ok

INSERT INTO Score VALUES (1, 100);
-- @expect: payload Insert
-- @json: 1

INSERT INTO Bonus VALUES (1, 5);
-- @expect: payload Insert
-- @json: 1

-- @name: a bare column name resolves against the target table first
UPDATE Score SET points = points + Bonus.points FROM Bonus WHERE id = Bonus.id;
-- @expect: payload Update
-- @json: 1

SELECT points FROM Score;
-- @expect:
-- | points: I64 |
-- | ----------- |
-- | 105         |

CREATE TABLE Pair (id INTEGER PRIMARY KEY, v INTEGER);
-- @expect: ok

CREATE TABLE Single (x INTEGER);
-- @expect: ok

INSERT INTO Pair VALUES (1, 0), (2, 0);
-- @expect: payload Insert
-- @json: 2

INSERT INTO Single VALUES (42);
-- @expect: payload Insert
-- @json: 1

-- @name: without a WHERE clause every source row matches
UPDATE Pair SET v = x FROM Single;
-- @expect: payload Update
-- @json: 2

SELECT v FROM Pair ORDER BY id;
-- @expect:
-- | v: I64 |
-- | ------ |
-- | 42     |
-- | 42     |

CREATE TABLE Doc;
-- @expect: ok

INSERT INTO Doc VALUES ('{"a": 1}');
-- @expect: payload Insert
-- @json: 1

-- @name: a schemaless target rejects FROM
UPDATE Doc SET a = 2 FROM Single;
-- @expect: error Execute.SourceRequiresSchema
-- @json: "Doc"

-- @name: a schemaless source rejects FROM
UPDATE Pair SET v = 1 FROM Doc;
-- @expect: error Execute.SourceRequiresSchema
-- @json: "Doc"

-- @name: a joined source table is rejected
UPDATE Pair SET v = 1 FROM Single JOIN Bonus ON TRUE;
-- @expect: error Translate.JoinOnUpdateNotSupported

-- @name: a derived table as the FROM source is rejected
UPDATE Pair SET v = 1 FROM (SELECT x FROM Single) AS sub;
-- @expect: error Translate.UnsupportedTableFactor
-- @json: "(SELECT x FROM Single) AS sub"
