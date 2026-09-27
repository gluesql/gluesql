CREATE TABLE Item (
    id INTEGER NULL,
    flag BOOLEAN,
    ratio FLOAT NULL,
    number TEXT
)
-- @expect: payload Create

INSERT INTO Item VALUES (0, TRUE, NULL, '1')
-- @expect: payload Insert
-- @json: 1

SELECT CAST(LOWER(number) AS INTEGER) AS cast FROM Item
-- @expect:
-- | cast: I64 |
-- | --------- |
-- | 1         |

SELECT CAST(id AS BOOLEAN) AS cast FROM Item
-- @expect:
-- | cast: Bool |
-- | ---------- |
-- | false      |

SELECT CAST(flag AS TEXT) AS cast FROM Item
-- @expect:
-- | cast: Str |
-- | --------- |
-- | "TRUE"    |

SELECT CAST(ratio AS INTEGER) AS cast FROM Item
-- @expect:
-- | cast |
-- | ---- |
-- | NULL |

SELECT CAST(number AS BOOLEAN) FROM Item
-- @expect: error Value.ConvertFailed
-- @json:
-- {
--   "data_type": "Boolean",
--   "value": {
--     "Str": "1"
--   }
-- }

CREATE TABLE IntervalLog (
    id INTEGER,
    interval_str_1 TEXT,
    interval_str_2 TEXT
)
-- @expect: payload Create

INSERT INTO IntervalLog VALUES
    (1, '''1-2'' YEAR TO MONTH',         '''30'' MONTH'),
    (2, '''12'' DAY',                    '''35'' HOUR'),
    (3, '''12'' MINUTE',                 '''300'' SECOND'),
    (4, '''-3 14'' DAY TO HOUR',         '''3 12:30'' DAY TO MINUTE'),
    (5, '''3 14:00:00'' DAY TO SECOND',  '''3 12:30:12.1324'' DAY TO SECOND'),
    (6, '''12:00'' HOUR TO MINUTE',      '''-12:30:12'' HOUR TO SECOND'),
    (7, '''-1000-11'' YEAR TO MONTH',    '''-30:11'' MINUTE TO SECOND');
-- @expect: payload Insert
-- @json: 7

SELECT id, CAST(interval_str_1 as INTERVAL) as stoi_1, CAST(interval_str_2 as INTERVAL) as stoi_2 FROM IntervalLog;
-- @expect:
-- | id: I64 | stoi_1: Interval           | stoi_2: Interval                  |
-- | ------- | -------------------------- | --------------------------------- |
-- | 1       | "'1-2' YEAR TO MONTH"      | "'2-6' YEAR TO MONTH"             |
-- | 2       | "'12' DAY"                 | "'1 11' DAY TO HOUR"              |
-- | 3       | "'12' MINUTE"              | "'5' MINUTE"                      |
-- | 4       | "'-3 14' DAY TO HOUR"      | "'3 12:30' DAY TO MINUTE"         |
-- | 5       | "'3 14' DAY TO HOUR"       | "'3 12:30:12.1324' DAY TO SECOND" |
-- | 6       | "'12' HOUR"                | "'-12:30:12' HOUR TO SECOND"      |
-- | 7       | "'-1000-11' YEAR TO MONTH" | "'-30:11' MINUTE TO SECOND"       |

SELECT CAST(1 AS STRING FORMAT 'ASCII') AS bytes_to_string;
-- @expect: error Translate.UnsupportedCastFormat
-- @json: "'ASCII'"
