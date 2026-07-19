SELECT
    SIGN(2) AS SIGN1,
    SIGN(-2) AS SIGN2,
    SIGN(+2) AS SIGN3
    ;
-- @expect:
-- | SIGN1: I8 | SIGN2: I8 | SIGN3: I8 |
-- | 1         | -1        | 1         |

SELECT
    SIGN(2.0) AS SIGN1,
    SIGN(-2.0) AS SIGN2,
    SIGN(+2.0) AS SIGN3
    ;
-- @expect:
-- | SIGN1: I8 | SIGN2: I8 | SIGN3: I8 |
-- | 1         | -1        | 1         |

SELECT
    SIGN(0.0) AS SIGN1,
    SIGN(-0.0) AS SIGN2,
    SIGN(+0.0) AS SIGN3
    ;
-- @expect:
-- | SIGN1: I8 | SIGN2: I8 | SIGN3: I8 |
-- | 0         | 0         | 0         |

SELECT
    SIGN(0) AS SIGN1,
    SIGN(-0) AS SIGN2,
    SIGN(+0) AS SIGN3
    ;
-- @expect:
-- | SIGN1: I8 | SIGN2: I8 | SIGN3: I8 |
-- | 0         | 0         | 0         |

SELECT SIGN('string') AS SIGN
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "SIGN"

SELECT SIGN(NULL) AS sign
-- @expect:
-- | sign |
-- | NULL |

SELECT SIGN(TRUE) AS sign
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "SIGN"

SELECT SIGN(FALSE) AS sign
-- @expect: error Evaluate.FunctionRequiresFloatValue
-- @json: "SIGN"

SELECT SIGN('string', 'string2') AS SIGN
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "SIGN"
-- }
