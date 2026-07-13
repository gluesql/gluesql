CREATE TABLE TimeLog (
    id INTEGER,
    time1 TIME,
    time2 TIME
)
-- expect: ok

INSERT INTO TimeLog VALUES
    (1, '12:30:00', '13:31:01.123'),
    (2, '9:2:1', 'AM 08:02:01.001'),
    (3, 'PM 2:59', '9:00:00 AM');
-- expect: ok

SELECT id, time1, time2 FROM TimeLog;
-- expect:
-- | id: I64 | time1: Time | time2: Time    |
-- | 1       | "12:30:00"  | "13:31:01.123" |
-- | 2       | "09:02:01"  | "08:02:01.001" |
-- | 3       | "14:59:00"  | "09:00:00"     |

SELECT * FROM TimeLog WHERE time1 > time2
-- expect:
-- | id: I64 | time1: Time | time2: Time    |
-- | 2       | "09:02:01"  | "08:02:01.001" |
-- | 3       | "14:59:00"  | "09:00:00"     |

SELECT * FROM TimeLog WHERE time1 <= time2
-- expect:
-- | id: I64 | time1: Time | time2: Time    |
-- | 1       | "12:30:00"  | "13:31:01.123" |

SELECT * FROM TimeLog WHERE time1 = TIME '14:59:00'
-- expect:
-- | id: I64 | time1: Time | time2: Time |
-- | 3       | "14:59:00"  | "09:00:00"  |

SELECT * FROM TimeLog WHERE time1 < '1:00 PM'
-- expect:
-- | id: I64 | time1: Time | time2: Time    |
-- | 1       | "12:30:00"  | "13:31:01.123" |
-- | 2       | "09:02:01"  | "08:02:01.001" |

SELECT * FROM TimeLog WHERE TIME '23:00:00.123' > 'PM 1:00';
-- expect:
-- | id: I64 | time1: Time | time2: Time    |
-- | 1       | "12:30:00"  | "13:31:01.123" |
-- | 2       | "09:02:01"  | "08:02:01.001" |
-- | 3       | "14:59:00"  | "09:00:00"     |

SELECT
    id,
    time1 - time2 AS time_sub,
    time1 + INTERVAL '1' HOUR AS add,
    time2 - INTERVAL '250' MINUTE AS sub
FROM TimeLog;
-- expect:
-- | id: I64 | time_sub: Interval               | add: Time  | sub: Time      |
-- | 1       | "'-01:01:01.123' HOUR TO SECOND" | "13:30:00" | "09:21:01.123" |
-- | 2       | "'59:59.999' MINUTE TO SECOND"   | "10:02:01" | "03:52:01.001" |
-- | 3       | "'05:59' HOUR TO MINUTE"         | "15:59:00" | "04:50:00"     |

SELECT
    id,
    DATE '2021-01-05' + time2 AS timestamp
FROM TimeLog LIMIT 1;
-- expect:
-- | id: I64 | timestamp: Timestamp      |
-- | 1       | "2021-01-05 13:31:01.123" |

SELECT * FROM TimeLog WHERE time1 > time2 + INTERVAL '1' YEAR
-- expect: error Interval.AddYearOrMonthToTime
-- {
--   "interval": {
--     "Month": 12
--   },
--   "time": "13:31:01.123"
-- }

SELECT * FROM TimeLog WHERE time1 > time2 - INTERVAL '1-2' YEAR TO MONTH
-- expect: error Interval.SubtractYearOrMonthToTime
-- {
--   "interval": {
--     "Month": 14
--   },
--   "time": "13:31:01.123"
-- }

INSERT INTO TimeLog VALUES (1, '12345-678', '20:05:01')
-- expect: error Evaluate.TextParseFailed
-- {
--   "data_type": "Time",
--   "literal": "12345-678"
-- }
