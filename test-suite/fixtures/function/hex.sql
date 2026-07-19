VALUES(HEX('Hello World'))
-- @expect:
-- | column1: Str             |
-- | ------------------------ |
-- | "48656C6C6F20576F726C64" |

VALUES(HEX('ABC'))
-- @expect:
-- | column1: Str |
-- | ------------ |
-- | "414243"     |

VALUES(HEX(''))
-- @expect:
-- | column1: Str |
-- | ------------ |
-- | ""           |

VALUES(HEX('228'))
-- @expect:
-- | column1: Str |
-- | ------------ |
-- | "323238"     |

VALUES(HEX(228))
-- @expect:
-- | column1: Str |
-- | ------------ |
-- | "E4"         |

VALUES(HEX(0))
-- @expect:
-- | column1: Str |
-- | ------------ |
-- | "0"          |

VALUES(HEX(-123))
-- @expect:
-- | column1: Str       |
-- | ------------------ |
-- | "FFFFFFFFFFFFFF85" |

VALUES(HEX(3.14))
-- @expect: error Evaluate.FunctionRequiresIntegerOrStringValue
-- @json: "HEX"

VALUES(HEX(NULL))
-- @expect:
-- | column1 |
-- | ------- |
-- | NULL    |

VALUES(HEX())
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "HEX"
-- }

VALUES(HEX('test', 'extra'))
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 1,
--   "found": 2,
--   "name": "HEX"
-- }
