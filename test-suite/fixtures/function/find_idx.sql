CREATE TABLE Meal (menu Text null)
-- expect: payload Create

INSERT INTO Meal VALUES ('pork')
-- expect: payload Insert
-- 1

INSERT INTO Meal VALUES ('burger')
-- expect: payload Insert
-- 1

SELECT FIND_IDX(menu, 'rg') AS test FROM Meal
-- expect:
-- | test: I64 |
-- | 0         |
-- | 3         |

SELECT FIND_IDX(menu, 'r', 4) AS test FROM Meal
-- expect:
-- | test: I64 |
-- | 0         |
-- | 6         |

SELECT FIND_IDX('cheese', '') AS test
-- expect:
-- | test: I64 |
-- | 0         |

SELECT FIND_IDX('cheese', 's') AS test
-- expect:
-- | test: I64 |
-- | 5         |

SELECT FIND_IDX('cheese burger', 'e', 5) AS test
-- expect:
-- | test: I64 |
-- | 6         |

SELECT FIND_IDX('cheese', NULL) AS test
-- expect:
-- | test |
-- | NULL |

SELECT FIND_IDX('cheese', 1) AS test
-- expect: error Evaluate.FunctionRequiresStringValue
-- "FIND_IDX"

SELECT FIND_IDX('cheese', 's', '5') AS test
-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "FIND_IDX"

SELECT FIND_IDX('cheese', 's', -1) AS test
-- expect: error Value.NonPositiveIntegerOffsetInFindIdx
-- "-1"
