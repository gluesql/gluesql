CREATE TABLE IdxValue (
    id INTEGER NULL,
    time TIME NULL,
    flag BOOLEAN
);
-- expect: ok

INSERT INTO IdxValue
VALUES
    (NULL, '01:30 PM', True),
    (1, '12:10 AM', False),
    (2, NULL, True);
-- expect: ok

CREATE INDEX idx_id ON IdxValue (id);
-- expect: payload CreateIndex

CREATE INDEX idx_time ON IdxValue (time);
-- expect: payload CreateIndex

CREATE INDEX idx_flag ON IdxValue (flag);
-- expect: payload CreateIndex

SELECT * FROM IdxValue WHERE id = 1;
-- expect-index: idx_id = 1
-- expect:
-- | id: I64 | time: Time | flag: Bool |
-- | 1       | 00:10:00   | false      |

SELECT * FROM IdxValue WHERE time <= TIME '13:30:00';
-- expect-index: idx_time <= TIME '13:30:00'
-- expect:
-- | id     | time           | flag        |
-- | I64(1) | Time(00:10:00) | Bool(false) |
-- | NULL   | Time(13:30:00) | Bool(true)  |

SELECT * FROM IdxValue WHERE flag = ('ABC' IS NULL);
-- expect-index: idx_flag = ('ABC' IS NULL)
-- expect:
-- | id     | time           | flag        |
-- | I64(1) | Time(00:10:00) | Bool(false) |

SELECT * FROM IdxValue WHERE flag = (100 IS NOT NULL);
-- expect-index: idx_flag = (100 IS NOT NULL)
-- expect:
-- | id     | time           | flag       |
-- | NULL   | Time(13:30:00) | Bool(true) |
-- | I64(2) | NULL           | Bool(true) |

SELECT * FROM IdxValue WHERE id = +1;
-- expect-index: idx_id = +1
-- expect:
-- | id: I64 | time: Time | flag: Bool |
-- | 1       | 00:10:00   | false      |

SELECT * FROM IdxValue WHERE id = CAST('1' AS INTEGER);
-- expect-index: idx_id = CAST('1' AS INTEGER)
-- expect:
-- | id: I64 | time: Time | flag: Bool |
-- | 1       | 00:10:00   | false      |

SELECT * FROM IdxValue WHERE id = (1);
-- expect-index: idx_id = (1)
-- expect:
-- | id: I64 | time: Time | flag: Bool |
-- | 1       | 00:10:00   | false      |

SELECT * FROM IdxValue WHERE id = 1 + 1 * 5 / 5;
-- expect-index: idx_id = 1 + 1 * 5 / 5
-- expect:
-- | id     | time | flag       |
-- | I64(2) | NULL | Bool(true) |

SELECT * FROM IdxValue WHERE flag = (True AND False);
-- expect-index: idx_flag = (True AND False)
-- expect:
-- | id: I64 | time: Time | flag: Bool |
-- | 1       | 00:10:00   | false      |
