VALUES(MD5('GlueSQL'))

-- expect:
-- | column1: Str                       |
-- | "4274ecec96f3ee59b51b168dc6137231" |

VALUES(MD5('GlueSQL Hi'))

-- expect:
-- | column1: Str                       |
-- | "eab30259ac1a92b66794f301a6ac3ff3" |

VALUES(MD5(NULL))

-- expect:
-- | column1 |
-- | NULL    |

VALUES(MD5())

-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "MD5"
-- }
