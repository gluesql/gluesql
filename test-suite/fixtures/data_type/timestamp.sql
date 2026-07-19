CREATE TABLE TimestampLog (
    id INTEGER,
    t1 TIMESTAMP,
    t2 TIMESTAMP
)
-- @expect: ok

INSERT INTO TimestampLog VALUES
    (1, '2020-06-11 11:23:11Z',           '2021-03-01'),
    (2, '2020-09-30 12:00:00 -07:00',     '1989-01-01T00:01:00+09:00'),
    (3, '2021-04-30T07:00:00.1234-17:00', '2021-05-01T09:00:00.1234+09:00');
-- @expect: ok

SELECT id, t1, t2 FROM TimestampLog
-- @expect:
-- | id: I64 | t1: Timestamp                | t2: Timestamp                |
-- | 1       | "2020-06-11 11:23:11"        | "2021-03-01 00:00:00"        |
-- | 2       | "2020-09-30 19:00:00"        | "1988-12-31 15:01:00"        |
-- | 3       | "2021-05-01 00:00:00.123400" | "2021-05-01 00:00:00.123400" |

SELECT * FROM TimestampLog WHERE t1 > t2
-- @expect:
-- | id: I64 | t1: Timestamp         | t2: Timestamp         |
-- | 2       | "2020-09-30 19:00:00" | "1988-12-31 15:01:00" |

SELECT * FROM TimestampLog WHERE t1 = t2
-- @expect:
-- | id: I64 | t1: Timestamp                | t2: Timestamp                |
-- | 3       | "2021-05-01 00:00:00.123400" | "2021-05-01 00:00:00.123400" |

SELECT * FROM TimestampLog WHERE t1 = '2020-06-11T14:23:11+0300';
-- @expect:
-- | id: I64 | t1: Timestamp         | t2: Timestamp         |
-- | 1       | "2020-06-11 11:23:11" | "2021-03-01 00:00:00" |

SELECT * FROM TimestampLog WHERE t2 < TIMESTAMP '2000-01-01';
-- @expect:
-- | id: I64 | t1: Timestamp         | t2: Timestamp         |
-- | 2       | "2020-09-30 19:00:00" | "1988-12-31 15:01:00" |

SELECT * FROM TimestampLog WHERE TIMESTAMP '1999-01-03' < '2000-01-01';
-- @expect:
-- | id: I64 | t1: Timestamp                | t2: Timestamp                |
-- | 1       | "2020-06-11 11:23:11"        | "2021-03-01 00:00:00"        |
-- | 2       | "2020-09-30 19:00:00"        | "1988-12-31 15:01:00"        |
-- | 3       | "2021-05-01 00:00:00.123400" | "2021-05-01 00:00:00.123400" |

SELECT id, t1 - t2 AS timestamp_sub FROM TimestampLog;
-- @expect:
-- | id: I64 | timestamp_sub: Interval         |
-- | 1       | "'-262 12:36:49' DAY TO SECOND" |
-- | 2       | "'11596 03:59' DAY TO MINUTE"   |
-- | 3       | "'00:00' MINUTE TO SECOND"      |

SELECT
    id,
    t1 - INTERVAL '1' DAY AS sub,
    t2 + INTERVAL '1' MONTH AS add
FROM TimestampLog;
-- @expect:
-- | id: I64 | sub: Timestamp               | add: Timestamp               |
-- | 1       | "2020-06-10 11:23:11"        | "2021-04-01 00:00:00"        |
-- | 2       | "2020-09-29 19:00:00"        | "1989-01-31 15:01:00"        |
-- | 3       | "2021-04-30 00:00:00.123400" | "2021-06-01 00:00:00.123400" |

INSERT INTO TimestampLog VALUES (1, '12345-678', '2021-05-01')
-- @expect: error Evaluate.TextParseFailed
-- @json:
-- {
--   "data_type": "Timestamp",
--   "literal": "12345-678"
-- }
