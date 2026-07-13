CREATE TABLE SingleItem (id integer null, int8 int8 null, dec decimal null,
    dt date null, mystring Text null,
    mybool Boolean null, myfloat float null,
    mytime time null, mytimestamp timestamp null)
-- expect: payload Create

INSERT INTO SingleItem VALUES (0, 1, 2, '2022-05-23', 'this is a string', true, 3.15,
    '01:02:03', '1970-01-01 00:00:00 -00:00')
-- expect: payload Insert
-- 1

INSERT INTO SingleItem VALUES (null, null, null, null, null, null, null, null, null)
-- expect: payload Insert
-- 1

SELECT IFNULL(id, 1) AS myid, IFNULL(int8, 2) AS int8, IFNULL(dec, 3)
FROM SingleItem WHERE id IS NOT NULL
-- expect:
-- | myid: I64 | int8: I8 | IFNULL(dec, 3): Decimal |
-- | 0         | 1        | 2                       |

SELECT ifnull(id, 1) AS ID, IFNULL(int8, 2) AS INT8, IFNULL(dec, 3)
FROM SingleItem WHERE id IS NULL
-- expect:
-- | ID: I64 | INT8: I64 | IFNULL(dec, 3): I64 |
-- | 1       | 2         | 3                   |

SELECT ifnull(dt, '2000-01-01') AS mydate, ifnull(mystring, 'blah') AS name
FROM SingleItem WHERE id IS NOT NULL
-- expect:
-- | mydate: Date | name: Str          |
-- | "2022-05-23" | "this is a string" |

SELECT IFNULL(dt, '2000-01-01') AS mydate, IFNULL(mystring, 'blah') AS name
FROM SingleItem where id is null
-- expect:
-- | mydate: Str  | name: Str |
-- | "2000-01-01" | "blah"    |

SELECT IFNULL(mybool, 'YES') AS mybool, IFNULL(myfloat, 'NO') AS myfloat
FROM SingleItem WHERE id IS NOT NULL
-- expect:
-- | mybool: Bool | myfloat: F64 |
-- | true         | 3.15         |

SELECT IFNULL(mybool, 'YES') AS mybool, IFNULL(myfloat, 'NO') AS myfloat
FROM SingleItem WHERE id IS NULL
-- expect:
-- | mybool: Str | myfloat: Str |
-- | "YES"       | "NO"         |

SELECT
    IFNULL(mytime, 'YES') AS mytime,
    IFNULL(mytimestamp, 'NO') AS mytimestamp
FROM SingleItem
WHERE id IS NOT NULL
-- expect:
-- | mytime: Time | mytimestamp: Timestamp |
-- | "01:02:03"   | "1970-01-01 00:00:00"  |

SELECT IFNULL(mytime, 'YES') AS mytime, IFNULL(mytimestamp, 'NO') AS mytimestamp
FROM SingleItem WHERE id IS NULL
-- expect:
-- | mytime: Str | mytimestamp: Str |
-- | "YES"       | "NO"             |
