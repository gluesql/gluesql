CREATE TABLE Test (
    id INTEGER
)
-- @expect: payload Create

INSERT INTO Test VALUES (1), (2), (3), (4), (5), (6), (7), (8);
-- @expect: payload Insert
-- @json: 8

SELECT * FROM Test LIMIT 10;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 2       |
-- | 3       |
-- | 4       |
-- | 5       |
-- | 6       |
-- | 7       |
-- | 8       |

SELECT * FROM Test LIMIT 10 OFFSET 1;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |
-- | 3       |
-- | 4       |
-- | 5       |
-- | 6       |
-- | 7       |
-- | 8       |

SELECT * FROM Test OFFSET 2;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 3       |
-- | 4       |
-- | 5       |
-- | 6       |
-- | 7       |
-- | 8       |

SELECT * FROM Test OFFSET 10;
-- @expect:
-- | id  |
-- | --- |

SELECT * FROM Test LIMIT 3;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 2       |
-- | 3       |

SELECT * FROM Test LIMIT 4 OFFSET 3;
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 4       |
-- | 5       |
-- | 6       |
-- | 7       |

SELECT * FROM Test ORDER BY id DESC LIMIT 3
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 8       |
-- | 7       |
-- | 6       |

SELECT id, COUNT(*) as c FROM Test GROUP BY id LIMIT 3 OFFSET 2
-- @expect:
-- | id: I64 | c: I64 |
-- | ------- | ------ |
-- | 3       | 1      |
-- | 4       | 1      |
-- | 5       | 1      |

CREATE TABLE InsertTest (
    case_no INTEGER,
    id INTEGER
)
-- @expect: payload Create

INSERT INTO InsertTest SELECT 1, id FROM Test OFFSET 1;
-- @expect: payload Insert
-- @json: 7

SELECT id FROM InsertTest WHERE case_no = 1
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |
-- | 3       |
-- | 4       |
-- | 5       |
-- | 6       |
-- | 7       |
-- | 8       |

INSERT INTO InsertTest SELECT 2, id FROM Test LIMIT 1;
-- @expect: payload Insert
-- @json: 1

SELECT id FROM InsertTest WHERE case_no = 2
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |

INSERT INTO InsertTest SELECT 3, id FROM Test ORDER BY id LIMIT 1 OFFSET 1;
-- @expect: payload Insert
-- @json: 1

SELECT id FROM InsertTest WHERE case_no = 3
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |

INSERT INTO InsertTest VALUES (4, 1), (4, 2), (4, 3), (4, 4) LIMIT 1;
-- @expect: payload Insert
-- @json: 1

SELECT id FROM InsertTest WHERE case_no = 4
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |

INSERT INTO InsertTest VALUES (5, 1), (5, 2), (5, 3), (5, 4) OFFSET 1;
-- @expect: payload Insert
-- @json: 3

SELECT id FROM InsertTest WHERE case_no = 5
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |
-- | 3       |
-- | 4       |

INSERT INTO InsertTest VALUES (6, 1), (6, 2), (6, 3), (6, 4) LIMIT 3 OFFSET 2;
-- @expect: payload Insert
-- @json: 2

SELECT id FROM InsertTest WHERE case_no = 6
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 3       |
-- | 4       |

INSERT INTO InsertTest VALUES (7, 2), (7, 1), (7, 3) ORDER BY column2 DESC LIMIT 1 OFFSET 1;
-- @expect: payload Insert
-- @json: 1

SELECT id FROM InsertTest WHERE case_no = 7
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |

INSERT INTO InsertTest VALUES (8, 2), (8, 1), (8, 3) ORDER BY column2 DESC;
-- @expect: payload Insert
-- @json: 3

SELECT id FROM InsertTest WHERE case_no = 8
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 3       |
-- | 2       |
-- | 1       |

INSERT INTO InsertTest VALUES (9, 2), (9, 1), (9, 3) ORDER BY column2 DESC LIMIT 2;
-- @expect: payload Insert
-- @json: 2

SELECT id FROM InsertTest WHERE case_no = 9
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 3       |
-- | 2       |

INSERT INTO InsertTest VALUES (10, 2), (10, 1), (10, 3) ORDER BY column2 DESC OFFSET 1;
-- @expect: payload Insert
-- @json: 2

SELECT id FROM InsertTest WHERE case_no = 10
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |
-- | 1       |

INSERT INTO InsertTest (id, case_no) VALUES (3, 11), (1, 11), (2, 11) ORDER BY column1;
-- @expect: payload Insert
-- @json: 3

SELECT id FROM InsertTest WHERE case_no = 11
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 2       |
-- | 3       |

INSERT INTO InsertTest VALUES (12, 2), (12, 1), (12, 2) ORDER BY column1, column2 DESC;
-- @expect: payload Insert
-- @json: 3

SELECT id FROM InsertTest WHERE case_no = 12
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 2       |
-- | 2       |
-- | 1       |

CREATE TABLE InsertDefault (
    case_no INTEGER DEFAULT 13,
    id INTEGER
)
-- @expect: payload Create

INSERT INTO InsertDefault (id) VALUES (2), (1), (3) ORDER BY column1 LIMIT 1;
-- @expect: payload Insert
-- @json: 1

SELECT * FROM InsertDefault
-- @expect:
-- | case_no: I64 | id: I64 |
-- | ------------ | ------- |
-- | 13           | 1       |

SELECT * FROM MissingTable LIMIT 1;
-- @expect: error Fetch.TableNotFound
-- @json: "MissingTable"
