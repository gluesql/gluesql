VALUES(TO_DATE('2017-06-15', '%Y-%m-%d'))

-- expect:
-- | column1: Date |
-- | "2017-06-15"  |

VALUES(TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S'))

-- expect:
-- | column1: Timestamp    |
-- | "2015-09-05 23:56:04" |

VALUES(TO_TIME('23:56:04', '%H:%M:%S'))

-- expect:
-- | column1: Time |
-- | "23:56:04"    |

SELECT TO_DATE('2017-06-15','%Y-%m-%d') AS date

-- expect:
-- | date: Date   |
-- | "2017-06-15" |

SELECT TO_DATE('2017-jun-15','%Y-%b-%d') AS date

-- expect:
-- | date: Date   |
-- | "2017-06-15" |

SELECT TO_TIME('23:56:04','%H:%M:%S') AS time

-- expect:
-- | time: Time |
-- | "23:56:04" |

SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S') AS timestamp

-- expect:
-- | timestamp: Timestamp  |
-- | "2015-09-05 23:56:04" |

SELECT TO_DATE(DATE '2017-06-15','%Y-%m-%d') AS date

-- expect: error Evaluate.FunctionRequiresStringValue
-- "TO_DATE"

SELECT TO_TIMESTAMP(TIMESTAMP '2015-09-05 23:56:04','%Y-%m-%d') AS date

-- expect: error Evaluate.FunctionRequiresStringValue
-- "TO_TIMESTAMP"

SELECT TO_TIME(TIME '23:56:04','%H:%M:%S') AS date

-- expect: error Evaluate.FunctionRequiresStringValue
-- "TO_TIME"

SELECT TO_DATE('2015-09-05', '%Y-%m') AS date

-- expect: error Evaluate.FormatParseError

SELECT TO_TIME('23:56', '%H:%M:%S') AS time

-- expect: error Evaluate.FormatParseError

SELECT TO_TIMESTAMP('2015-05 23', '%Y-%d %H') AS timestamp

-- expect: error Evaluate.FormatParseError

SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%S') AS timestamp

-- expect: error Evaluate.FormatParseError

SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%S') AS timestamp

-- expect: error Evaluate.FormatParseError

SELECT TO_TIMESTAMP('2015-14-05 23:56:12','%Y-%m-%d %H:%M:%%S') AS timestamp;

-- expect: error Evaluate.FormatParseError

SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%M') AS timestamp

-- expect: error Evaluate.FormatParseError

SELECT TO_TIMESTAMP('2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%') AS timestamp

-- expect: error Evaluate.FormatParseError
