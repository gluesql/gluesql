CREATE TABLE LastDay (
    id INTEGER,
    date DATE,
    timestamp TIMESTAMP
);
-- @expect: ok

INSERT INTO LastDay (id, date) VALUES (1, LAST_DAY(DATE '2017-12-15'));
-- @expect: ok

-- @name: Should insert the last day of the month that a given date belongs to
SELECT date FROM LastDay WHERE id = 1;
-- @expect:
-- | date: Date   |
-- | "2017-12-31" |

INSERT INTO LastDay (id, date) VALUES (2, DATE '2017-01-01');
-- @expect: ok

-- @name: Should return the last day of the month that a retrieved date belongs to
SELECT LAST_DAY(date) as date FROM LastDay WHERE id = 2;
-- @expect:
-- | date: Date   |
-- | "2017-01-31" |

INSERT INTO LastDay (id, date) VALUES (3, LAST_DAY(TIMESTAMP '2017-12-15 12:12:20'));
-- @expect: ok

-- @name: Should insert the last day of the month that a given timestamp belongs to
SELECT date FROM LastDay WHERE id = 3;
-- @expect:
-- | date: Date   |
-- | "2017-12-31" |

INSERT INTO LastDay (id, timestamp) VALUES (4, TIMESTAMP '2017-01-01 12:12:20');
-- @expect: ok

-- @name: Should return the last day of the month that a retrieved timestamp belongs to
SELECT LAST_DAY(timestamp) as date FROM LastDay WHERE id = 4;
-- @expect:
-- | date: Date   |
-- | "2017-01-31" |

-- @name: Should only give date or timestamp value to LAST_DAY function
VALUES (LAST_DAY('dfafsdf3243252454325342'));
-- @expect: error Evaluate.FunctionRequiresDateOrDateTimeValue
-- @json: "LAST_DAY"
