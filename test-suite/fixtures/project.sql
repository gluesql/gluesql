CREATE TABLE ProjectUser (
    id INTEGER,
    name TEXT
);
-- @expect: ok

CREATE TABLE ProjectItem (
    id INTEGER,
    player_id INTEGER,
    quantity INTEGER
);
-- @expect: ok

DELETE FROM ProjectUser
-- @expect: ok

DELETE FROM ProjectItem
-- @expect: ok

INSERT INTO ProjectUser (id, name) VALUES
    (1, 'Taehoon'),
    (2,    'Mike'),
    (3,   'Jorno');
-- @expect: ok

INSERT INTO ProjectItem (id, player_id, quantity) VALUES
    (101, 1, 1),
    (102, 2, 4),
    (103, 2, 9),
    (104, 3, 2),
    (105, 3, 1);
-- @expect: ok

SELECT 1 FROM ProjectUser
-- @expect:
-- | 1: I64 |
-- | ------ |
-- | 1      |
-- | 1      |
-- | 1      |

SELECT id, name FROM ProjectUser
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 1       | "Taehoon" |
-- | 2       | "Mike"    |
-- | 3       | "Jorno"   |

SELECT player_id, quantity FROM ProjectItem
-- @expect:
-- | player_id: I64 | quantity: I64 |
-- | -------------- | ------------- |
-- | 1              | 1             |
-- | 2              | 4             |
-- | 2              | 9             |
-- | 3              | 2             |
-- | 3              | 1             |

SELECT player_id, player_id FROM ProjectItem
-- @expect:
-- | player_id: I64 | player_id: I64 |
-- | -------------- | -------------- |
-- | 1              | 1              |
-- | 2              | 2              |
-- | 2              | 2              |
-- | 3              | 3              |
-- | 3              | 3              |

SELECT u.id, i.id, player_id
FROM ProjectUser u
JOIN ProjectItem i ON u.id = 1 AND u.id = i.player_id
-- @expect:
-- | id: I64 | id: I64 | player_id: I64 |
-- | ------- | ------- | -------------- |
-- | 1       | 101     | 1              |

SELECT i.*, u.name
FROM ProjectUser u
JOIN ProjectItem i ON u.id = 2 AND u.id = i.player_id
-- @expect:
-- | id: I64 | player_id: I64 | quantity: I64 | name: Str |
-- | ------- | -------------- | ------------- | --------- |
-- | 102     | 2              | 4             | "Mike"    |
-- | 103     | 2              | 9             | "Mike"    |

SELECT u.*, i.*
FROM ProjectUser u
JOIN ProjectItem i ON u.id = i.player_id
-- @expect:
-- | id: I64 | name: Str | id: I64 | player_id: I64 | quantity: I64 |
-- | ------- | --------- | ------- | -------------- | ------------- |
-- | 1       | "Taehoon" | 101     | 1              | 1             |
-- | 2       | "Mike"    | 102     | 2              | 4             |
-- | 2       | "Mike"    | 103     | 2              | 9             |
-- | 3       | "Jorno"   | 104     | 3              | 2             |
-- | 3       | "Jorno"   | 105     | 3              | 1             |

SELECT id as Ident, name FROM ProjectUser
-- @expect:
-- | Ident: I64 | name: Str |
-- | ---------- | --------- |
-- | 1          | "Taehoon" |
-- | 2          | "Mike"    |
-- | 3          | "Jorno"   |

SELECT (1 + 2) as foo, 2+id+2*100-1 as Ident, name FROM ProjectUser
-- @expect:
-- | foo: I64 | Ident: I64 | name: Str |
-- | -------- | ---------- | --------- |
-- | 3        | 202        | "Taehoon" |
-- | 3        | 203        | "Mike"    |
-- | 3        | 204        | "Jorno"   |

SELECT id FROM ProjectUser
WHERE id IN (
    SELECT ProjectUser.id FROM ProjectItem
    WHERE quantity > 5 AND ProjectUser.id = player_id
);
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |

SELECT Whatever.* FROM ProjectUser
-- @expect: error Fetch.TableAliasNotFound
-- @json: "Whatever"

SELECT noname FROM ProjectUser
-- @expect: error Evaluate.IdentifierNotFound
-- @json: "noname"

SELECT (SELECT id FROM ProjectItem) as id FROM ProjectItem
-- @expect: error Evaluate.MoreThanOneRowReturned

SELECT (SELECT 1,2)
-- @expect: error Evaluate.MoreThanOneColumnReturned
