SELECT * FROM SERIES(3)
-- @expect:
-- | N: I64 |
-- | 1      |
-- | 2      |
-- | 3      |

SELECT * FROM sErIeS(3)
-- @expect:
-- | N: I64 |
-- | 1      |
-- | 2      |
-- | 3      |

SELECT S.* FROM SERIES(3) as S
-- @expect:
-- | N: I64 |
-- | 1      |
-- | 2      |
-- | 3      |

SELECT * FROM SERIES(+3)
-- @expect:
-- | N: I64 |
-- | 1      |
-- | 2      |
-- | 3      |

CREATE TABLE SeriesTable AS SELECT * FROM SERIES(3)
-- @expect: payload Create

SELECT * FROM SeriesTable
-- @expect:
-- | N: I64 |
-- | 1      |
-- | 2      |
-- | 3      |

SELECT * FROM SERIES(0)
-- @expect:
-- | N |

SELECT * FROM SERIES
-- @expect: error Fetch.TableNotFound
-- @json: "SERIES"

SELECT * FROM SERIES()
-- @expect: error Translate.LackOfArgs

SELECT * FROM SERIES(-1)
-- @expect: error Fetch.SeriesSizeWrong
-- @json: -1

SELECT 1, 'a', true, 1 + 2, 'a' || 'b'
-- @expect:
-- | 1: I64 | 'a': Str | true: Bool | 1 + 2: I64 | "'a' \u007c\u007c 'b'": Str |
-- | 1      | "a"      | true       | 3          | "ab"                        |

SELECT (SELECT 'Hello')
-- @expect:
-- | (SELECT 'Hello'): Str |
-- | "Hello"               |

SELECT 1 AS id, (SELECT MAX(N) FROM SERIES(3)) AS max
-- @expect:
-- | id: I64 | max: I64 |
-- | 1       | 3        |

SELECT * FROM (SELECT 1) AS Drived
-- @expect:
-- | 1: I64 |
-- | 1      |

SELECT *
-- @expect:
-- | N: I64 |
-- | 1      |

CREATE TABLE TargetTable AS SELECT 1
-- @expect: payload Create

SELECT * FROM TargetTable
-- @expect:
-- | 1: I64 |
-- | 1      |

CREATE TABLE TargetTableWithExpressions AS SELECT 3, 4
-- @expect: payload Create

SELECT * FROM TargetTableWithExpressions
-- @expect:
-- | 3: I64 | 4: I64 |
-- | 3      | 4      |
