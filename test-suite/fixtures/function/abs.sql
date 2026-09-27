SELECT ABS(1) AS ABS1,
    ABS(-1) AS ABS2,
    ABS(+1) AS ABS3
-- @expect:
-- | ABS1: I64 | ABS2: I64 | ABS3: I64 |
-- | --------- | --------- | --------- |
-- | 1         | 1         | 1         |

SELECT ABS(1.5) AS ABS1,
    ABS(-1.5) AS ABS2,
    ABS(+1.5) AS ABS3;
-- @expect:
-- | ABS1: F64 | ABS2: F64 | ABS3: F64 |
-- | --------- | --------- | --------- |
-- | 1.5       | 1.5       | 1.5       |

SELECT ABS(0) AS ABS1,
    ABS(-0) AS ABS2,
    ABS(+0) AS ABS3;
-- @expect:
-- | ABS1: I64 | ABS2: I64 | ABS3: I64 |
-- | --------- | --------- | --------- |
-- | 0         | 0         | 0         |

CREATE TABLE SingleItem (id integer, int8 int8, dec decimal)
-- @expect: payload Create

INSERT INTO SingleItem VALUES (0, -1, -2)
-- @expect: payload Insert
-- @json: 1

SELECT ABS(id) AS ABS1,
    ABS(int8) AS ABS2,
    ABS(dec) AS ABS3
FROM SingleItem
-- @expect:
-- | ABS1: I64 | ABS2: I8 | ABS3: Decimal |
-- | --------- | -------- | ------------- |
-- | 0         | 1        | 2             |

SELECT ABS('string') AS ABS FROM SingleItem
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ABS"

SELECT ABS(NULL) AS ABS;
-- @expect:
-- | ABS  |
-- | ---- |
-- | NULL |

SELECT ABS(TRUE) AS ABS;
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ABS"

SELECT ABS(FALSE) AS ABS;
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "ABS"

SELECT ABS('string', 'string2') AS ABS
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "ABS"
-- }
