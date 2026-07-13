CREATE TABLE Item (id INTEGER, name TEXT);
-- expect: payload Create

INSERT INTO
    Item (id, name)
    VALUES
        (1, 'Harry'), (2, 'Ron'), (3, 'Hermione');
-- expect: payload Insert
-- 3

SELECT CASE id
        WHEN 1 THEN name
        WHEN 2 THEN name
        WHEN 4 THEN name
        ELSE 'Malfoy' END
    AS case FROM Item;
-- expect:
-- | case: Str |
-- | "Harry"   |
-- | "Ron"     |
-- | "Malfoy"  |

SELECT CASE id
        WHEN 1 THEN name
        WHEN 2 THEN name
        WHEN 4 THEN name
        END
    AS case FROM Item;
-- expect:
-- | case: Str |
-- | "Harry"   |
-- | "Ron"     |
-- | NULL      |

SELECT CASE
        WHEN name = 'Harry' THEN id
        WHEN name = 'Ron' THEN id
        WHEN name = 'Hermione' THEN id
        ELSE 404 END
    AS case FROM Item;
-- expect:
-- | case: I64 |
-- | 1         |
-- | 2         |
-- | 3         |

SELECT CASE
        WHEN name = 'Harry' THEN id
        WHEN name = 'Ron' THEN id
        WHEN name = 'Hermion' THEN id
        END
    AS case FROM Item;
-- expect:
-- | case: I64 |
-- | 1         |
-- | 2         |
-- | NULL      |

SELECT CASE
        WHEN (name = 'Harry') OR (name = 'Ron') THEN (id + 1)
        WHEN name = ('Hermi' || 'one') THEN (id + 2)
        ELSE 404 END
    AS case FROM Item;
-- expect:
-- | case: I64 |
-- | 2         |
-- | 3         |
-- | 5         |

SELECT CASE 1 COLLATE Item
        WHEN name = 'Harry' THEN id
        WHEN name = 'Ron' THEN id
        WHEN 'Hermione' THEN id
        END
    AS case FROM Item;
-- expect: error Translate.UnsupportedExpr
-- "1 COLLATE Item"

SELECT 1 COLLATE Item FROM Item;
-- expect: error Translate.UnsupportedExpr
-- "1 COLLATE Item"
