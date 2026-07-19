VALUES(FORMAT(DATE '2017-06-15', '%Y-%m'))
-- @expect:
-- | column1: Str |
-- | "2017-06"    |

SELECT FORMAT(DATE '2017-06-15','%Y-%m') AS date
-- @expect:
-- | date: Str |
-- | "2017-06" |

SELECT FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y-%m-%d %H:%M:%S') AS timestamp
-- @expect:
-- | timestamp: Str        |
-- | "2015-09-05 23:56:04" |

SELECT FORMAT(TIME '23:56:04','%H:%M') AS time
-- @expect:
-- | time: Str |
-- | "23:56"   |

SELECT
    FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%Y') AS year
    ,FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%m') AS month
    ,FORMAT(TIMESTAMP '2015-09-05 23:56:04', '%d') AS day
-- @expect:
-- | year: Str | month: Str | day: Str |
-- | "2015"    | "09"       | "05"     |

SELECT FORMAT('2015-09-05 23:56:04', '%Y-%m-%d %H') AS timestamp
-- @expect: error Evaluate.UnsupportedExprForFormatFunction
-- @json: "2015-09-05 23:56:04"
