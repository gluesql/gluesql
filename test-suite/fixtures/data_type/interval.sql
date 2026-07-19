CREATE TABLE IntervalLog (
    id INTEGER,
    interval1 INTERVAL,
    interval2 INTERVAL
)
-- @expect: ok

INSERT INTO IntervalLog VALUES
    (1, INTERVAL '1-2' YEAR TO MONTH,         INTERVAL 30 MONTH),
    (2, INTERVAL 12 DAY,                      INTERVAL '35' HOUR),
    (3, INTERVAL '12' MINUTE,                 INTERVAL 300 SECOND),
    (4, INTERVAL '-3 14' DAY TO HOUR,         INTERVAL '3 12:30' DAY TO MINUTE),
    (5, INTERVAL '3 14:00:00' DAY TO SECOND,  INTERVAL '3 12:30:12.1324' DAY TO SECOND),
    (6, INTERVAL '12:00' HOUR TO MINUTE,      INTERVAL '-12:30:12' HOUR TO SECOND),
    (7, INTERVAL '-1000-11' YEAR TO MONTH,    INTERVAL '-30:11' MINUTE TO SECOND);
-- @expect: ok

SELECT * FROM IntervalLog;
-- @expect:
-- | id: I64 | interval1: Interval        | interval2: Interval               |
-- | 1       | "'1-2' YEAR TO MONTH"      | "'2-6' YEAR TO MONTH"             |
-- | 2       | "'12' DAY"                 | "'1 11' DAY TO HOUR"              |
-- | 3       | "'12' MINUTE"              | "'5' MINUTE"                      |
-- | 4       | "'-3 14' DAY TO HOUR"      | "'3 12:30' DAY TO MINUTE"         |
-- | 5       | "'3 14' DAY TO HOUR"       | "'3 12:30:12.1324' DAY TO SECOND" |
-- | 6       | "'12' HOUR"                | "'-12:30:12' HOUR TO SECOND"      |
-- | 7       | "'-1000-11' YEAR TO MONTH" | "'-30:11' MINUTE TO SECOND"       |

SELECT
    id,
    interval1 * 2 AS i1,
    interval2 - INTERVAL '-3' YEAR AS i2
FROM IntervalLog WHERE id = 1
-- @expect:
-- | id: I64 | i1: Interval          | i2: Interval          |
-- | 1       | "'2-4' YEAR TO MONTH" | "'5-6' YEAR TO MONTH" |

SELECT
    id,
    interval1 / 3 AS i1,
    interval2 - INTERVAL 3600 SECOND AS i2,
    INTERVAL (20 + 10) SECOND + INTERVAL (10 * 3) SECOND AS i3
FROM IntervalLog WHERE id = 2;
-- @expect:
-- | id: I64 | i1: Interval | i2: Interval         | i3: Interval |
-- | 2       | "'4' DAY"    | "'1 10' DAY TO HOUR" | "'1' MINUTE" |

INSERT INTO IntervalLog VALUES (1, INTERVAL '20:00' MINUTE TO HOUR, INTERVAL '1-2' YEAR TO MONTH)
-- @expect: error Interval.UnsupportedRange
-- @json:
-- [
--   "Minute",
--   "Hour"
-- ]

SELECT INTERVAL '1' YEAR + INTERVAL '1' HOUR FROM IntervalLog;
-- @expect: error Interval.AddBetweenYearToMonthAndHourToSecond

SELECT INTERVAL '1' YEAR - INTERVAL '1' HOUR FROM IntervalLog;
-- @expect: error Interval.SubtractBetweenYearToMonthAndHourToSecond

SELECT INTERVAL '1.4' YEAR FROM IntervalLog;
-- @expect: error Interval.FailedToParseInteger
-- @json: "1.4"

SELECT INTERVAL '1.4ab' HOUR FROM IntervalLog;
-- @expect: error Interval.FailedToParseDecimal
-- @json: "1.4ab"

SELECT INTERVAL '111:34' HOUR TO MINUTE FROM IntervalLog;
-- @expect: error Interval.FailedToParseTime
-- @json: "111:34"

SELECT INTERVAL '111' YEAR TO MONTH FROM IntervalLog;
-- @expect: error Interval.FailedToParseYearToMonth
-- @json: "111"

SELECT INTERVAL '111' DAY TO HOUR FROM IntervalLog;
-- @expect: error Interval.FailedToParseDayToHour
-- @json: "111"

SELECT INTERVAL '111' DAY TO HOUR FROM IntervalLog;
-- @expect: error Interval.FailedToParseDayToHour
-- @json: "111"

SELECT INTERVAL '111' DAY TO MINUTE FROM IntervalLog;
-- @expect: error Interval.FailedToParseDayToMinute
-- @json: "111"

SELECT INTERVAL '111' DAY TO Second FROM IntervalLog;
-- @expect: error Interval.FailedToParseDayToSecond
-- @json: "111"
