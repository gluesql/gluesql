CREATE TABLE Allegro (
    id INTEGER PRIMARY KEY,
    name TEXT
);
-- expect: ok

INSERT INTO Allegro VALUES (1, 'hello'), (3, 'world');
-- expect: payload Insert
-- 2

SELECT id, name FROM Allegro
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "hello"   |
-- | 3       | "world"   |

SELECT id, name FROM Allegro WHERE id = 1
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "hello"   |

SELECT id, name FROM Allegro WHERE id < 2
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "hello"   |

SELECT a.id
    FROM Allegro a
    JOIN Allegro a2
    WHERE a.id = a2.id;
-- expect:
-- | id: I64 |
-- | 1       |
-- | 3       |

SELECT id FROM Allegro WHERE id IN (
    SELECT id FROM Allegro WHERE id = id
);
-- expect:
-- | id: I64 |
-- | 1       |
-- | 3       |

INSERT INTO Allegro VALUES (5, 'neon'), (2, 'foo'), (4, 'bar');
-- expect: ok

SELECT id, name FROM Allegro
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "hello"   |
-- | 2       | "foo"     |
-- | 3       | "world"   |
-- | 4       | "bar"     |
-- | 5       | "neon"    |

SELECT id, name FROM Allegro WHERE id % 2 = 0
-- expect:
-- | id: I64 | name: Str |
-- | 2       | "foo"     |
-- | 4       | "bar"     |

SELECT id, name FROM Allegro WHERE id = 4
-- expect:
-- | id: I64 | name: Str |
-- | 4       | "bar"     |

DELETE FROM Allegro WHERE id > 3
-- expect: ok

SELECT id, name FROM Allegro
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "hello"   |
-- | 2       | "foo"     |
-- | 3       | "world"   |

CREATE TABLE Strslice (
    name TEXT PRIMARY KEY
);
-- expect: ok

INSERT INTO Strslice VALUES (SUBSTR(SUBSTR('foo', 1), 1));
-- expect: ok

-- name: PRIMARY KEY includes UNIQUE constraint
INSERT INTO Allegro VALUES (1, 'another hello');
-- expect: error Validate.DuplicateEntryOnPrimaryKeyField
-- {
--   "I64": 1
-- }

-- name: PRIMARY KEY includes NOT NULL constraint
INSERT INTO Allegro VALUES (NULL, 'hello');
-- expect: error Value.NullValueOnNotNullField

-- name: UPDATE is not allowed for PRIMARY KEY applied column
UPDATE Allegro SET id = 100 WHERE id = 1
-- expect: error Update.UpdateOnPrimaryKeyNotSupported
-- "id"
