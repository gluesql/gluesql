SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 13           |

SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 2016         |

SELECT EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 12           |

SELECT EXTRACT(DAY FROM TIMESTAMP '2016-12-31 13:30:15') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 31           |

SELECT EXTRACT(MINUTE FROM TIMESTAMP '2016-12-31 13:30:15') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 30           |

SELECT EXTRACT(SECOND FROM TIMESTAMP '2016-12-31 13:30:15') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 15           |

SELECT EXTRACT(SECOND FROM TIME '17:12:28') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 28           |

SELECT EXTRACT(DAY FROM DATE '2021-10-06') as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 6            |

SELECT EXTRACT(YEAR FROM INTERVAL '3' YEAR) as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 3            |

SELECT EXTRACT(MONTH FROM INTERVAL '4' MONTH) as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 4            |

SELECT EXTRACT(DAY FROM INTERVAL '5' DAY) as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 5            |

SELECT EXTRACT(HOUR FROM INTERVAL '6' HOUR) as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 6            |

SELECT EXTRACT(MINUTE FROM INTERVAL '7' MINUTE) as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 7            |

SELECT EXTRACT(SECOND FROM INTERVAL '8' SECOND) as extract
-- @expect:
-- | extract: I64 |
-- | ------------ |
-- | 8            |

CREATE TABLE Item (number TEXT)
-- @expect: payload Create

INSERT INTO Item VALUES ('1')
-- @expect: payload Insert
-- @json: 1

SELECT EXTRACT(HOUR FROM number) as extract FROM Item
-- @expect: error Value.ExtractFormatNotMatched
-- @json:
-- {
--   "field": "Hour",
--   "value": {
--     "Str": "1"
--   }
-- }

SELECT EXTRACT(HOUR FROM INTERVAL '7' YEAR) as extract
-- @expect: error Interval.FailedToExtract

SELECT EXTRACT(HOUR FROM 100)
-- @expect: error Value.ExtractFormatNotMatched
-- @json:
-- {
--   "field": "Hour",
--   "value": {
--     "I64": 100
--   }
-- }

SELECT EXTRACT(microseconds FROM '2011-01-1');
-- @expect: error Translate.UnsupportedDateTimeField
-- @json: "MICROSECONDS"
