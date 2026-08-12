CREATE TABLE Player (
    id INTEGER PRIMARY KEY,
    team_id INTEGER,
    active BOOLEAN
);
-- @expect: ok

CREATE TABLE Badge (
    player_id INTEGER
);
-- @expect: ok

CREATE TABLE Team (
    id INTEGER
);
-- @expect: ok

-- @name: primary key predicate becomes a direct lookup
EXPLAIN
SELECT team_id
FROM Player
WHERE id = 1;
-- @expect: explain
-- • project
-- │ columns: team_id
-- │
-- └── • scan Player
--       access: primary key
--       key: 1

-- @name: query clauses form the planned execution pipeline
EXPLAIN
SELECT Player.team_id, COUNT(*) AS player_count
FROM Player
INNER JOIN Badge ON Player.id = Badge.player_id
LEFT JOIN Team ON Player.team_id = Team.id
WHERE Player.active = TRUE
GROUP BY Player.team_id
ORDER BY player_count DESC
LIMIT 10 OFFSET 5;
-- @expect: explain
-- • limit
-- │ count: 10
-- │
-- └── • offset
--     │ count: 5
--     │
--     └── • sort
--         │ order: player_count DESC
--         │
--         └── • project
--             │ columns: Player.team_id, COUNT(*) AS player_count
--             │
--             └── • aggregate
--                 │ group by: Player.team_id
--                 │ aggregates: COUNT(*)
--                 │
--                 └── • filter
--                     │ expression: Player.active = TRUE
--                     │
--                     └── • hash join (left outer)
--                         │ equality: Player.team_id = Team.id
--                         │
--                         ├── • hash join (inner)
--                         │   │ equality: Player.id = Badge.player_id
--                         │   │
--                         │   ├── • scan Player
--                         │   │     access: full scan
--                         │   │
--                         │   └── • scan Badge
--                         │         access: full scan
--                         │
--                         └── • scan Team
--                               access: full scan

-- @name: expression subqueries are referenced from the main plan
EXPLAIN
SELECT
    id,
    (SELECT COUNT(*) AS total FROM Badge) AS badge_count
FROM Player
WHERE id IN (SELECT player_id FROM Badge)
AND EXISTS (
    SELECT *
    FROM Badge
    WHERE Badge.player_id = Player.id
);
-- @expect: explain
-- • root
-- ├── • project
-- │   │ columns: id, @S1 AS badge_count
-- │   │
-- │   └── • filter
-- │       │ expression: id IN (@S2) AND EXISTS (@S3)
-- │       │
-- │       └── • scan Player
-- │             access: full scan
-- │
-- ├── • subquery
-- │   │ id: @S1
-- │   │ exec mode: one row
-- │   │
-- │   └── • project
-- │       │ columns: COUNT(*) AS total
-- │       │
-- │       └── • aggregate
-- │           │ aggregates: COUNT(*)
-- │           │
-- │           └── • scan Badge
-- │                 access: full scan
-- │
-- ├── • subquery
-- │   │ id: @S2
-- │   │ exec mode: all rows
-- │   │
-- │   └── • project
-- │       │ columns: player_id
-- │       │
-- │       └── • scan Badge
-- │             access: full scan
-- │
-- └── • subquery
--     │ id: @S3
--     │ exec mode: exists
--     │
--     └── • project
--         │ columns: *
--         │
--         └── • filter
--             │ expression: Badge.player_id = Player.id
--             │
--             └── • scan Badge
--                   access: full scan
