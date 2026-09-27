-- @name: plus test on general case
SELECT ADD_MONTH('2017-06-15',1) AS test;
-- @expect:
-- | test: Date   |
-- | ------------ |
-- | "2017-07-15" |

-- @name: minus test on general case
SELECT ADD_MONTH('2017-06-15',-1) AS test;
-- @expect:
-- | test: Date   |
-- | ------------ |
-- | "2017-05-15" |

-- @name: the last day of February test
SELECT ADD_MONTH('2017-01-31',1) AS test;
-- @expect:
-- | test: Date   |
-- | ------------ |
-- | "2017-02-28" |

-- @name: year change test
SELECT ADD_MONTH('2017-01-31',13) AS test;
-- @expect:
-- | test: Date   |
-- | ------------ |
-- | "2018-02-28" |

-- @name: zero test
SELECT ADD_MONTH('2017-01-31',0) AS test;
-- @expect:
-- | test: Date   |
-- | ------------ |
-- | "2017-01-31" |

-- @name: out of range test with i64::MAX
SELECT ADD_MONTH('2017-01-31',9223372036854775807) AS test;
-- @expect: error Evaluate.I64ToU32ConversionFailure
-- @json: "ADD_MONTH"

-- @name: out of range test
SELECT ADD_MONTH('2017-01-31',10000000000000000000) AS test;
-- @expect: error Evaluate.FunctionRequiresIntegerValue
-- @json: "ADD_MONTH"

-- @name: out of range test with i32::MAX
SELECT ADD_MONTH('2017-01-31',2147483648) AS test;
-- @expect: error Evaluate.ChrFunctionRequiresIntegerValueInRange0To255

SELECT ADD_MONTH('2017-01-31-10',0) AS test;
-- @expect: error Evaluate.FormatParseError

SELECT ADD_MONTH('2017-01',0) AS test;
-- @expect: error Evaluate.FormatParseError

SELECT ADD_MONTH('2015-14-05',1) AS test
-- @expect: error Evaluate.FormatParseError
