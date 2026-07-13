CREATE TABLE Player (
    id INTEGER,
    name TEXT
);
-- expect: ok

CREATE TABLE Item (
    id INTEGER,
    quantity INTEGER,
    player_id INTEGER
);
-- expect: ok

INSERT INTO Player (id, name) VALUES
    (1, 'Taehoon'),
    (2,    'Mike'),
    (3,   'Jorno'),
    (4,   'Berry'),
    (5,    'Hwan');
-- expect: ok

INSERT INTO Item (id, quantity, player_id) VALUES
    (101, 1, 1),
    (102, 4, 2),
    (103, 9, 4);
-- expect: ok

SELECT p.id, i.id
    FROM Player p
    LEFT JOIN Item i
    ON p.id = i.player_id
-- expect:
-- | id: I64 | id: I64 |
-- | 1       | 101     |
-- | 2       | 102     |
-- | 3       | NULL    |
-- | 4       | 103     |
-- | 5       | NULL    |

SELECT p.id, player_id
    FROM Player p
    LEFT JOIN Item
    ON p.id = player_id
-- expect:
-- | id: I64 | player_id: I64 |
-- | 1       | 1              |
-- | 2       | 2              |
-- | 3       | NULL           |
-- | 4       | 4              |
-- | 5       | NULL           |

SELECT Item.*
    FROM Player p
    LEFT JOIN Item
    ON p.id = player_id
-- expect:
-- | id: I64 | quantity: I64 | player_id: I64 |
-- | 101     | 1             | 1              |
-- | 102     | 4             | 2              |
-- | NULL    | NULL          | NULL           |
-- | 103     | 9             | 4              |
-- | NULL    | NULL          | NULL           |

SELECT *
    FROM Player p
    LEFT JOIN Item
    ON p.id = player_id
-- expect:
-- | id: I64 | name: Str | id: I64 | quantity: I64 | player_id: I64 |
-- | 1       | "Taehoon" | 101     | 1             | 1              |
-- | 2       | "Mike"    | 102     | 4             | 2              |
-- | 3       | "Jorno"   | NULL    | NULL          | NULL           |
-- | 4       | "Berry"   | 103     | 9             | 4              |
-- | 5       | "Hwan"    | NULL    | NULL          | NULL           |

CREATE TABLE Users (id INTEGER, name TEXT);
-- expect: ok

INSERT INTO Users (id, name) VALUES (1, 'Harry');
-- expect: ok

CREATE TABLE Testers (id INTEGER, nickname TEXT);
-- expect: ok

INSERT INTO Testers (id, nickname) VALUES (1, 'Ron');
-- expect: ok

SELECT * FROM TableA JOIN TableA USING (id);
-- expect: error Translate.UnsupportedJoinConstraint
-- "USING"

SELECT * FROM TableA CROSS JOIN TableA as A;
-- expect: error Translate.UnsupportedJoinOperator
-- "CrossJoin"

SELECT id FROM Users JOIN Testers ON Users.id = Testers.id;
-- expect: error Plan.ColumnReferenceAmbiguous
-- "id"

SELECT id FROM Users A JOIN Users B on A.id = B.id
-- expect: error Plan.ColumnReferenceAmbiguous
-- "id"

INSERT INTO Users SELECT id FROM Users A JOIN Users B on A.id = B.id
-- expect: error Plan.ColumnReferenceAmbiguous
-- "id"

CREATE TABLE Ids AS SELECT id FROM Users A JOIN Users B on A.id = B.id
-- expect: error Plan.ColumnReferenceAmbiguous
-- "id"

CREATE TABLE JoinedUsers AS SELECT * FROM Users JOIN Testers ON Users.id = Testers.id
-- expect: error Alter.DuplicateColumnName
-- "id"

SELECT * FROM ProjectUser, ProjectItem
-- expect: error Translate.TooManyTables
