CREATE TABLE Base (
    id INTEGER,
    name TEXT
);
-- @expect: ok

CREATE TABLE Side (
    base_id INTEGER,
    label TEXT
);
-- @expect: ok

INSERT INTO Base VALUES (1, 'one'), (2, 'two'), (3, 'three');
-- @expect: ok

INSERT INTO Side VALUES (1, 'a'), (3, 'b'), (9, 'c');
-- @expect: ok

CREATE INDEX idx_base_id ON Base (id);
-- @expect: payload CreateIndex

-- @name: a LEFT JOIN narrows its base source with an index seek
SELECT id, label FROM Base LEFT JOIN Side ON base_id = id WHERE id = 1;
-- @expect-index: idx_base_id = 1
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 1       | "a"        |

-- @name: a RIGHT JOIN needs the complete left input, so the index seek is off
SELECT id, label FROM Base RIGHT JOIN Side ON base_id = id WHERE id = 1;
-- @expect-index: none
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 1       | "a"        |

-- @name: a LEFT JOIN reads its base source through an ordered index scan
SELECT id, label FROM Base LEFT JOIN Side ON base_id = id ORDER BY id;
-- @expect-index: idx_base_id
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 1       | "a"        |
-- | 2       | NULL       |
-- | 3       | "b"        |

-- @name: the ordered index scan is off under a RIGHT JOIN too
SELECT id, label FROM Base RIGHT JOIN Side ON base_id = id ORDER BY id;
-- @expect-index: none
-- @expect:
-- | id: I64 | label: Str |
-- | ------- | ---------- |
-- | 1       | "a"        |
-- | 3       | "b"        |
-- | NULL    | "c"        |

-- @name: pinning is per-plan, so a later query over the same table still seeks
SELECT id, name FROM Base WHERE id = 3;
-- @expect-index: idx_base_id = 3
-- @expect:
-- | id: I64 | name: Str |
-- | ------- | --------- |
-- | 3       | "three"   |

DROP TABLE Base;
-- @expect: ok

DROP TABLE Side;
-- @expect: ok
