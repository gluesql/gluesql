CREATE TABLE NullIdx (
    id INTEGER,
    date DATE,
    flag BOOLEAN
);
-- @expect: ok

INSERT INTO NullIdx (id, date, flag)
VALUES
    (1, '2020-03-20', True),
    (2, '2021-01-01', True),
    (3, '1989-02-01', False),
    (4, '2002-06-11', True),
    (5, '2030-03-01', False);
-- @expect: ok

CREATE INDEX idx_id ON NullIdx (id);
-- @expect: payload CreateIndex

CREATE INDEX idx_date ON NullIdx (date);
-- @expect: payload CreateIndex

SELECT id, date, flag FROM NullIdx
WHERE
    date < DATE '2040-12-24'
    AND flag = false;
-- @expect-index: idx_date < DATE '2040-12-24'
-- @expect:
-- | id: I64 | date: Date | flag: Bool |
-- | ------- | ---------- | ---------- |
-- | 3       | 1989-02-01 | false      |
-- | 5       | 2030-03-01 | false      |

SELECT * FROM NullIdx
WHERE
    flag = False
    AND date < DATE '2020-12-24';
-- @expect-index: idx_date < DATE '2020-12-24'
-- @expect:
-- | id: I64 | date: Date | flag: Bool |
-- | ------- | ---------- | ---------- |
-- | 3       | 1989-02-01 | false      |

SELECT * FROM NullIdx
WHERE
    flag = False
    AND DATE '2030-11-24' > date
    AND id > 1;
-- @expect-index: idx_date < DATE '2030-11-24'
-- @expect:
-- | id: I64 | date: Date | flag: Bool |
-- | ------- | ---------- | ---------- |
-- | 3       | 1989-02-01 | false      |
-- | 5       | 2030-03-01 | false      |

SELECT * FROM NullIdx
WHERE
    flag = False
    AND id > 1
    AND DATE '2030-11-24' > date;
-- @expect-index: idx_id > 1
-- @expect:
-- | id: I64 | date: Date | flag: Bool |
-- | ------- | ---------- | ---------- |
-- | 3       | 1989-02-01 | false      |
-- | 5       | 2030-03-01 | false      |

SELECT * FROM NullIdx
WHERE
    flag = False
    AND id * 2 > 6;
-- @expect-index: none
-- @expect:
-- | id: I64 | date: Date | flag: Bool |
-- | ------- | ---------- | ---------- |
-- | 5       | 2030-03-01 | false      |

SELECT * FROM NullIdx
WHERE
    flag = False
    AND id * 2 > 6
    AND (date = DATE '2030-03-01' AND flag != True);
-- @expect-index: idx_date = DATE '2030-03-01'
-- @expect:
-- | id: I64 | date: Date | flag: Bool |
-- | ------- | ---------- | ---------- |
-- | 5       | 2030-03-01 | false      |
