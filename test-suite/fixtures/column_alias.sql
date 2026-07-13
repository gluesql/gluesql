CREATE TABLE InnerTable (
    id INTEGER,
    name TEXT
)
-- expect: payload Create

CREATE TABLE User (
    id INTEGER,
    name TEXT
)
-- expect: payload Create

CREATE TABLE EmptyTable
-- expect: payload Create

INSERT INTO InnerTable VALUES (1, 'GLUE'), (2, 'SQL'), (3, 'SQL')
-- expect: payload Insert
-- 3

INSERT INTO User VALUES (1, 'Taehoon'), (2, 'Mike'), (3, 'Jorno')
-- expect: payload Insert
-- 3

SELECT * FROM InnerTable
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "GLUE"    |
-- | 2       | "SQL"     |
-- | 3       | "SQL"     |

SELECT * FROM User AS Table(a, b)
-- expect:
-- | a: I64 | b: Str    |
-- | 1      | "Taehoon" |
-- | 2      | "Mike"    |
-- | 3      | "Jorno"   |

SELECT * FROM User AS Table(a)
-- expect:
-- | a: I64 | name: Str |
-- | 1      | "Taehoon" |
-- | 2      | "Mike"    |
-- | 3      | "Jorno"   |

SELECT a FROM User AS Table(a, b)
-- expect:
-- | a: I64 |
-- | 1      |
-- | 2      |
-- | 3      |

Select * from User as Table(a, b, c)
-- expect: error Fetch.TooManyColumnAliases
-- [
--   "User",
--   2,
--   3
-- ]

SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a, b)
-- expect:
-- | a: I64 | b: Str |
-- | 1      | "GLUE" |
-- | 2      | "SQL"  |
-- | 3      | "SQL"  |

SELECT a, b FROM (SELECT * FROM InnerTable) AS InlineView(a, b)
-- expect:
-- | a: I64 | b: Str |
-- | 1      | "GLUE" |
-- | 2      | "SQL"  |
-- | 3      | "SQL"  |

SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a)
-- expect:
-- | a: I64 | name: Str |
-- | 1      | "GLUE"    |
-- | 2      | "SQL"     |
-- | 3      | "SQL"     |

SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a, b, c)
-- expect: error Fetch.TooManyColumnAliases
-- [
--   "InlineView",
--   2,
--   3
-- ]

SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id)
-- expect:
-- | id: I64 | column2: Str |
-- | 1       | "a"          |
-- | 2       | "b"          |

SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name)
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "a"       |
-- | 2       | "b"       |

SELECT Derived.id, Derived.name FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name)
-- expect:
-- | id: I64 | name: Str |
-- | 1       | "a"       |
-- | 2       | "b"       |

SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name, dummy)
-- expect: error Fetch.TooManyColumnAliases
-- [
--   "Derived",
--   2,
--   3
-- ]
