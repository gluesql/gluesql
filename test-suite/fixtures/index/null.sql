CREATE TABLE NullIdx (
    id INTEGER NULL,
    date DATE NULL,
    flag BOOLEAN NULL
);

-- expect: ok

INSERT INTO NullIdx (id, date, flag)
VALUES
    (NULL, NULL, True),
    (1, '2020-03-20', True),
    (2, NULL, NULL),
    (3, '1989-02-01', False),
    (4, NULL, True);

-- expect: ok

CREATE INDEX idx_id ON NullIdx (id);

-- expect: payload CreateIndex

CREATE INDEX idx_date ON NullIdx (date);

-- expect: payload CreateIndex

CREATE INDEX idx_flag ON NullIdx (flag);

-- expect: payload CreateIndex

SELECT id, date, flag FROM NullIdx WHERE date < DATE '2040-12-24';

-- expect-index: idx_date < DATE '2040-12-24'
-- expect:
-- | id: I64 | date: Date | flag: Bool |
-- | 3       | 1989-02-01 | false      |
-- | 1       | 2020-03-20 | true       |

SELECT id, date, flag FROM NullIdx WHERE date >= DATE '2040-12-24';

-- expect-index: idx_date >= DATE '2040-12-24'
-- expect:
-- | id     | date | flag       |
-- | NULL   | NULL | Bool(true) |
-- | I64(2) | NULL | NULL       |
-- | I64(4) | NULL | Bool(true) |

SELECT * FROM NullIdx WHERE flag = True;

-- expect-index: idx_flag = True
-- expect:
-- | id     | date             | flag       |
-- | NULL   | NULL             | Bool(true) |
-- | I64(1) | Date(2020-03-20) | Bool(true) |
-- | I64(4) | NULL             | Bool(true) |

SELECT * FROM NullIdx WHERE id > 2;

-- expect-index: idx_id > 2
-- expect:
-- | id     | date             | flag        |
-- | I64(3) | Date(1989-02-01) | Bool(false) |
-- | I64(4) | NULL             | Bool(true)  |
-- | NULL   | NULL             | Bool(true)  |

SELECT * FROM NullIdx WHERE id IS NULL;

-- expect-index: idx_id = NULL
-- expect:
-- | id   | date | flag       |
-- | NULL | NULL | Bool(true) |

SELECT id, date, flag FROM NullIdx WHERE date IS NOT NULL;

-- expect-index: idx_date < NULL
-- expect:
-- | id: I64 | date: Date | flag: Bool |
-- | 3       | 1989-02-01 | false      |
-- | 1       | 2020-03-20 | true       |

SELECT * FROM NullIdx WHERE id = NULL;

-- expect-index: none
-- expect:
-- | id | date | flag |
