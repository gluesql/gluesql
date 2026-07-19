CREATE TABLE DateLog (
    id INTEGER,
    date1 DATE,
    date2 DATE
)
-- @expect: ok

INSERT INTO DateLog VALUES
    (1, '2020-06-11', '2021-03-01'),
    (2, '2020-09-30', '1989-01-01'),
    (3, '2021-05-01', '2021-05-01');
-- @expect: ok

SELECT id, date1, date2 FROM DateLog
-- @expect:
-- | id: I64 | date1: Date  | date2: Date  |
-- | 1       | "2020-06-11" | "2021-03-01" |
-- | 2       | "2020-09-30" | "1989-01-01" |
-- | 3       | "2021-05-01" | "2021-05-01" |

SELECT * FROM DateLog WHERE date1 > date2
-- @expect:
-- | id: I64 | date1: Date  | date2: Date  |
-- | 2       | "2020-09-30" | "1989-01-01" |

SELECT * FROM DateLog WHERE date1 <= date2
-- @expect:
-- | id: I64 | date1: Date  | date2: Date  |
-- | 1       | "2020-06-11" | "2021-03-01" |
-- | 3       | "2021-05-01" | "2021-05-01" |

SELECT * FROM DateLog WHERE date1 = DATE '2020-06-11';
-- @expect:
-- | id: I64 | date1: Date  | date2: Date  |
-- | 1       | "2020-06-11" | "2021-03-01" |

SELECT * FROM DateLog WHERE date2 < '2000-01-01';
-- @expect:
-- | id: I64 | date1: Date  | date2: Date  |
-- | 2       | "2020-09-30" | "1989-01-01" |

SELECT * FROM DateLog WHERE '1999-01-03' < DATE '2000-01-01';
-- @expect:
-- | id: I64 | date1: Date  | date2: Date  |
-- | 1       | "2020-06-11" | "2021-03-01" |
-- | 2       | "2020-09-30" | "1989-01-01" |
-- | 3       | "2021-05-01" | "2021-05-01" |

SELECT
    id,
    date1 - date2 AS date_sub,
    date1 - INTERVAL '1' DAY AS sub,
    date2 + INTERVAL '1' MONTH AS add
FROM DateLog;
-- @expect:
-- | id: I64 | date_sub: Interval         | sub: Timestamp        | add: Timestamp        |
-- | 1       | "'-263' DAY"               | "2020-06-10 00:00:00" | "2021-04-01 00:00:00" |
-- | 2       | "'11595' DAY"              | "2020-09-29 00:00:00" | "1989-02-01 00:00:00" |
-- | 3       | "'00:00' MINUTE TO SECOND" | "2021-04-30 00:00:00" | "2021-06-01 00:00:00" |

INSERT INTO DateLog VALUES (1, '12345-678', '2021-05-01')
-- @expect: error Evaluate.TextParseFailed
-- @json:
-- {
--   "data_type": "Date",
--   "literal": "12345-678"
-- }
