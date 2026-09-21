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

-- @name: DELETE ... USING removes every target row that matches a source row
DELETE FROM Item USING Restock AS r WHERE id = r.item_id;
-- @expect: payload Delete
-- @json: 2

SELECT id FROM Item;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |

INSERT INTO Item VALUES (1, 'pen', 3), (3, 'pad', 7);
-- @expect: payload Insert
-- @json: 2

INSERT INTO Restock VALUES (1, 99);
-- @expect: payload Insert
-- @json: 1

-- @name: a target row matching several source rows is removed once
DELETE FROM Item USING Restock r WHERE id = r.item_id RETURNING id, name;
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "pen"     |
-- | 3       | "pad"     |

SELECT id FROM Item;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |

CREATE TABLE Keep (id INTEGER PRIMARY KEY);
-- @expect: ok

CREATE TABLE Empty (id INTEGER);
-- @expect: ok

INSERT INTO Keep VALUES (1), (2);
-- @expect: payload Insert
-- @json: 2

-- @name: with no source row nothing matches
DELETE FROM Keep USING Empty WHERE Keep.id = Empty.id;
-- @expect: payload Delete
-- @json: 0

INSERT INTO Empty VALUES (7);
-- @expect: payload Insert
-- @json: 1

-- @name: without a WHERE clause every source row matches
DELETE FROM Keep USING Empty;
-- @expect: payload Delete
-- @json: 2

SELECT id FROM Keep;
-- @expect:
-- | id: I64 |
-- | ------- |

CREATE TABLE Doc;
-- @expect: ok

INSERT INTO Doc VALUES ('{"a": 1}');
-- @expect: payload Insert
-- @json: 1

-- @name: a schemaless target rejects USING
DELETE FROM Doc USING Empty;
-- @expect: error Execute.SourceRequiresSchema
-- @json: "Doc"

-- @name: a schemaless source rejects USING
DELETE FROM Keep USING Doc;
-- @expect: error Execute.SourceRequiresSchema
-- @json: "Doc"

-- @name: only one source table is accepted
DELETE FROM Keep USING Empty, Restock;
-- @expect: error Translate.UnsupportedDeleteOption
-- @json: "USING clause"

-- @name: a joined source table is rejected
DELETE FROM Keep USING Empty JOIN Restock ON TRUE;
-- @expect: error Translate.JoinOnUpdateNotSupported
