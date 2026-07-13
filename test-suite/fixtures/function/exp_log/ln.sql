SELECT
    LN(64.0) as ln1,
    LN(0.04) as ln2
    ;
-- expect:
-- | ln1: F64           | ln2: F64            |
-- | 4.1588830833596715 | -3.2188758248682006 |

SELECT LN(10) as ln_with_int
-- expect:
-- | ln_with_int: F64  |
-- | 2.302585092994046 |

SELECT LN('string') AS log10
-- expect: error Evaluate.FunctionRequiresFloatValue
-- "LN"

SELECT LN(NULL) AS ln
-- expect:
-- | ln   |
-- | NULL |
