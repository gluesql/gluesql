CREATE TABLE Item (number TEXT)
-- expect: payload Create

INSERT INTO Item VALUES ('1')
-- expect: payload Insert
-- 1

CREATE TABLE test (mytext Text, myint8 Int8, myint Int, myfloat Float, mydec Decimal, mybool Boolean, mydate Date)
-- expect: payload Create

CREATE TABLE utest (mytext Text, myuint8 UINT8, myint Int, myfloat Float, mydec Decimal, mybool Boolean, mydate Date)
-- expect: payload Create

INSERT INTO utest VALUES ('foobar', 2, 2, 2.0, 2.0, true, '2001-09-11')
-- expect: payload Insert
-- 1

INSERT INTO test VALUES ('foobar', -2, 2, 2.0, 2.0, true, '2001-09-11')
-- expect: payload Insert
-- 1

SELECT CAST('TRUE' AS BOOLEAN) AS cast FROM Item
-- expect:
-- | cast: Bool |
-- | true       |

SELECT 1::BOOLEAN AS cast
-- expect:
-- | cast: Bool |
-- | true       |

SELECT CAST(1 AS BOOLEAN) AS cast FROM Item
-- expect:
-- | cast: Bool |
-- | true       |

SELECT CAST('asdf' AS BOOLEAN) AS cast FROM Item
-- expect: error Evaluate.TextCastFailed
-- {
--   "data_type": "Boolean",
--   "literal": "asdf"
-- }

SELECT CAST(3 AS BOOLEAN) AS cast FROM Item
-- expect: error Evaluate.NumberCastFailed
-- {
--   "data_type": "Boolean",
--   "literal": "3"
-- }

SELECT CAST(NULL AS BOOLEAN) AS cast FROM Item
-- expect:
-- | cast |
-- | NULL |

SELECT CAST('1' AS INTEGER) AS cast FROM Item
-- expect:
-- | cast: I64 |
-- | 1         |

SELECT CAST(SUBSTR('123', 2, 3) AS INTEGER) AS cast FROM Item
-- expect:
-- | cast: I64 |
-- | 23        |

SELECT CAST('foo' AS INTEGER) AS cast FROM Item
-- expect: error Evaluate.TextCastFailed
-- {
--   "data_type": "Int",
--   "literal": "foo"
-- }

SELECT CAST(1.1 AS INTEGER) AS cast FROM Item
-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int",
--   "literal": "1.1"
-- }

SELECT CAST(TRUE AS INTEGER) AS cast FROM Item
-- expect:
-- | cast: I64 |
-- | 1         |

SELECT CAST(NULL AS INTEGER) AS cast FROM Item
-- expect:
-- | cast |
-- | NULL |

SELECT CAST(255 AS INT8) AS cast FROM Item
-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int8",
--   "literal": "255"
-- }

SELECT CAST('foo' AS UINT8) AS cast FROM Item
-- expect: error Evaluate.TextCastFailed
-- {
--   "data_type": "Uint8",
--   "literal": "foo"
-- }

SELECT CAST(-1 AS UINT8) AS cast FROM Item
-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint8",
--   "literal": "-1"
-- }

SELECT CAST('foo' AS UINT16) AS cast FROM Item
-- expect: error Evaluate.TextCastFailed
-- {
--   "data_type": "Uint16",
--   "literal": "foo"
-- }

SELECT CAST(-1 AS UINT16) AS cast FROM Item
-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint16",
--   "literal": "-1"
-- }

SELECT CAST('1.1' AS FLOAT) AS cast FROM Item
-- expect:
-- | cast: F64 |
-- | 1.1       |

SELECT CAST(1 AS FLOAT) AS cast FROM Item
-- expect:
-- | cast: F64 |
-- | 1.0       |

SELECT CAST('foo' AS FLOAT) AS cast FROM Item
-- expect: error Evaluate.TextCastFailed
-- {
--   "data_type": "Float",
--   "literal": "foo"
-- }

SELECT CAST(TRUE AS FLOAT) AS cast FROM Item
-- expect:
-- | cast: F64 |
-- | 1.0       |

SELECT CAST(NULL AS FLOAT) AS cast FROM Item
-- expect:
-- | cast |
-- | NULL |

SELECT CAST(true AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1             |

SELECT CAST(false AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 0             |

SELECT CAST(number AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1             |

SELECT CAST('1.1' AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1.1           |

SELECT CAST(1 AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1.0           |

SELECT CAST(-1 AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | -1.0          |

SELECT CAST('foo' AS Decimal) AS cast FROM Item
-- expect: error Evaluate.TextCastFailed
-- {
--   "data_type": "Decimal",
--   "literal": "foo"
-- }

SELECT CAST(NULL AS Decimal) AS cast FROM Item
-- expect:
-- | cast |
-- | NULL |

SELECT CAST(true AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1             |

SELECT CAST(false AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 0             |

SELECT CAST(number AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1             |

SELECT CAST('1.1' AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1.1           |

SELECT CAST(1 AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | 1.0           |

SELECT CAST(-1 AS Decimal) AS cast FROM Item
-- expect:
-- | cast: Decimal |
-- | -1.0          |

SELECT CAST('foo' AS Decimal) AS cast FROM Item
-- expect: error Evaluate.TextCastFailed
-- {
--   "data_type": "Decimal",
--   "literal": "foo"
-- }

SELECT CAST(NULL AS Decimal) AS cast FROM Item
-- expect:
-- | cast |
-- | NULL |

SELECT CAST(mytext AS Decimal) AS cast FROM test
-- expect: error Value.ConvertFailed
-- {
--   "data_type": "Decimal",
--   "value": {
--     "Str": "foobar"
--   }
-- }

SELECT CAST(myint8 AS Decimal) AS cast FROM test
-- expect:
-- | cast: Decimal |
-- | -2            |

SELECT CAST(myuint8 AS Decimal) AS cast FROM utest
-- expect:
-- | cast: Decimal |
-- | 2             |

SELECT CAST(myint AS Decimal) AS cast FROM test
-- expect:
-- | cast: Decimal |
-- | 2             |

SELECT CAST(myfloat AS Decimal) AS cast FROM test
-- expect:
-- | cast: Decimal |
-- | 2             |

SELECT CAST(mydec AS Decimal) AS cast FROM test
-- expect:
-- | cast: Decimal |
-- | 2             |

SELECT CAST(mybool AS Decimal) AS cast FROM test
-- expect:
-- | cast: Decimal |
-- | 1             |

SELECT CAST(not(mybool) AS Decimal) AS cast FROM test
-- expect:
-- | cast: Decimal |
-- | 0             |

SELECT CAST(mydate AS Decimal) AS cast FROM test
-- expect: error Value.ConvertFailed
-- {
--   "data_type": "Decimal",
--   "value": {
--     "Date": "2001-09-11"
--   }
-- }

SELECT CAST(1 AS TEXT) AS cast FROM Item
-- expect:
-- | cast: Str |
-- | "1"       |

SELECT CAST(1.1 AS TEXT) AS cast FROM Item
-- expect:
-- | cast: Str |
-- | "1.1"     |

SELECT CAST(TRUE AS TEXT) AS cast FROM Item
-- expect:
-- | cast: Str |
-- | "TRUE"    |

SELECT CAST(NULL AS TEXT) AS cast FROM Item
-- expect:
-- | cast |
-- | NULL |

SELECT CAST(NULL AS INTERVAL) AS cast FROM Item
-- expect:
-- | cast |
-- | NULL |

SELECT
    CAST('''1-2'' YEAR TO MONTH' as INTERVAL) as stoi_1,
    CAST('''12'' DAY' as INTERVAL) as stoi_2,
    CAST('''12'' MINUTE' as INTERVAL) as stoi_3,
    CAST('''-3 14'' DAY TO HOUR' as INTERVAL) as stoi_4,
    CAST('''3 14:00:00'' DAY TO SECOND' as INTERVAL) as stoi_5,
    CAST('''12:00'' HOUR TO MINUTE' as INTERVAL) as stoi_6,
    CAST('''-1000-11'' YEAR TO MONTH' as INTERVAL) as stoi_7,
    CAST('''30'' MONTH' as INTERVAL) as stoi_8,
    CAST('''35'' HOUR' as INTERVAL) as stoi_9,
    CAST('''300'' SECOND' as INTERVAL) as stoi_10,
    CAST('''3 12:30'' DAY TO MINUTE' as INTERVAL) as stoi_11,
    CAST('''3 12:30:12.1324'' DAY TO SECOND' as INTERVAL) as stoi_12,
    CAST('''-12:30:12'' HOUR TO SECOND' as INTERVAL) as stoi_13,
    CAST('''-30:11'' MINUTE TO SECOND' as INTERVAL) as stoi_14
FROM Item
-- expect:
-- | stoi_1: Interval      | stoi_2: Interval | stoi_3: Interval | stoi_4: Interval      | stoi_5: Interval     | stoi_6: Interval | stoi_7: Interval           | stoi_8: Interval      | stoi_9: Interval     | stoi_10: Interval | stoi_11: Interval         | stoi_12: Interval                 | stoi_13: Interval            | stoi_14: Interval           |
-- | "'1-2' YEAR TO MONTH" | "'12' DAY"       | "'12' MINUTE"    | "'-3 14' DAY TO HOUR" | "'3 14' DAY TO HOUR" | "'12' HOUR"      | "'-1000-11' YEAR TO MONTH" | "'2-6' YEAR TO MONTH" | "'1 11' DAY TO HOUR" | "'5' MINUTE"      | "'3 12:30' DAY TO MINUTE" | "'3 12:30:12.1324' DAY TO SECOND" | "'-12:30:12' HOUR TO SECOND" | "'-30:11' MINUTE TO SECOND" |

SELECT CAST('2021-08-25' AS DATE) AS cast FROM Item
-- expect:
-- | cast: Date   |
-- | "2021-08-25" |

SELECT CAST('08-25-2021' AS DATE) AS cast FROM Item
-- expect:
-- | cast: Date   |
-- | "2021-08-25" |

SELECT CAST('2021-08-025' AS DATE) FROM Item
-- expect: error Evaluate.TextParseFailed
-- {
--   "data_type": "Date",
--   "literal": "2021-08-025"
-- }

SELECT CAST('AM 8:05' AS TIME) AS cast FROM Item
-- expect:
-- | cast: Time |
-- | "08:05:00" |

SELECT CAST('AM 08:05' AS TIME) AS cast FROM Item
-- expect:
-- | cast: Time |
-- | "08:05:00" |

SELECT CAST('AM 8:05:30' AS TIME) AS cast FROM Item
-- expect:
-- | cast: Time |
-- | "08:05:30" |

SELECT CAST('AM 8:05:30.9' AS TIME) AS cast FROM Item
-- expect:
-- | cast: Time     |
-- | "08:05:30.900" |

SELECT CAST('8:05:30.9 AM' AS TIME) AS cast FROM Item
-- expect:
-- | cast: Time     |
-- | "08:05:30.900" |

SELECT CAST('25:08:05' AS TIME) AS cast FROM Item
-- expect: error Evaluate.TextParseFailed
-- {
--   "data_type": "Time",
--   "literal": "25:08:05"
-- }

SELECT CAST('2021-08-25 08:05:30' AS TIMESTAMP) AS cast FROM Item
-- expect:
-- | cast: Timestamp       |
-- | "2021-08-25 08:05:30" |

SELECT CAST('2021-08-25 08:05:30.9' AS TIMESTAMP) AS cast FROM Item
-- expect:
-- | cast: Timestamp           |
-- | "2021-08-25 08:05:30.900" |

SELECT CAST('2021-13-25 08:05:30' AS TIMESTAMP) AS cast FROM Item
-- expect: error Evaluate.TextParseFailed
-- {
--   "data_type": "Timestamp",
--   "literal": "2021-13-25 08:05:30"
-- }
