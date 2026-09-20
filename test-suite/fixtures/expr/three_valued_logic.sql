CREATE TABLE Flags (id INTEGER, a BOOLEAN, b BOOLEAN)
-- @expect: ok

INSERT INTO Flags VALUES (1, TRUE, NULL), (2, FALSE, NULL), (3, NULL, NULL), (4, NULL, TRUE), (5, NULL, FALSE)
-- @expect: ok

SELECT id FROM Flags WHERE a OR b
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 4       |

SELECT id FROM Flags WHERE a AND b
-- @expect:
-- | id: I64 |
-- | ------- |

SELECT id FROM Flags WHERE TRUE OR NULL
-- @expect:
-- | id: I64 |
-- | ------- |
-- | 1       |
-- | 2       |
-- | 3       |
-- | 4       |
-- | 5       |

SELECT id FROM Flags WHERE FALSE AND NULL
-- @expect:
-- | id: I64 |
-- | ------- |

SELECT id, a OR b AS either, a AND b AS both FROM Flags
-- @expect:
-- | id: I64 | either: Bool | both: Bool |
-- | ------- | ------------ | ---------- |
-- | 1       | true         | NULL       |
-- | 2       | NULL         | false      |
-- | 3       | NULL         | NULL       |
-- | 4       | true         | NULL       |
-- | 5       | NULL         | false      |
