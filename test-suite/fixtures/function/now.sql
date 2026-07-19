CREATE TABLE Item (time TIMESTAMP DEFAULT NOW())
-- @expect: payload Create

INSERT INTO Item VALUES
    ('2021-10-13T06:42:40.364832862'),
    ('9999-12-31T23:59:40.364832862');
-- @expect: payload Insert
-- @json: 2

SELECT time FROM Item WHERE time > NOW();
-- @expect:
-- | time: Timestamp                 |
-- | ------------------------------- |
-- | "9999-12-31 23:59:40.364832862" |
