-- name: table with CURRENT_TIMESTAMP default
CREATE TABLE Item (timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
-- expect: payload Create

-- name: insert timestamp values
INSERT INTO Item VALUES
    ('2021-10-13T06:42:40.364832862'),
    ('9999-12-31T23:59:40.364832862');
-- expect: payload Insert
-- 2

-- name: filter by CURRENT_TIMESTAMP
SELECT timestamp FROM Item WHERE timestamp > CURRENT_TIMESTAMP;
-- expect:
-- | timestamp: Timestamp            |
-- | "9999-12-31 23:59:40.364832862" |
