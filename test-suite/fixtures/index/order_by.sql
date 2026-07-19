CREATE TABLE Test (
    id INTEGER,
    num INTEGER NULL,
    name TEXT
);
-- @expect: ok

INSERT INTO Test (id, num, name)
VALUES
    (1, 2, 'Hello'),
    (1, 9, 'Wild'),
    (3, NULL, 'World'),
    (4, 7, 'Monday');
-- @expect: ok

CREATE INDEX idx_name ON Test (name);
-- @expect: payload CreateIndex

CREATE INDEX idx_id_num_asc ON Test (id + num ASC);
-- @expect: payload CreateIndex

CREATE INDEX idx_num_desc ON Test (num DESC);
-- @expect: payload CreateIndex

SELECT * FROM Test ORDER BY name;
-- @expect-index: idx_name
-- @expect:
-- | id     | num    | name          |
-- | ------ | ------ | ------------- |
-- | I64(1) | I64(2) | Str("Hello")  |
-- | I64(4) | I64(7) | Str("Monday") |
-- | I64(1) | I64(9) | Str("Wild")   |
-- | I64(3) | NULL   | Str("World")  |

SELECT * FROM Test ORDER BY id + num;
-- @expect-index: idx_id_num_asc
-- @expect:
-- | id     | num    | name          |
-- | ------ | ------ | ------------- |
-- | I64(1) | I64(2) | Str("Hello")  |
-- | I64(1) | I64(9) | Str("Wild")   |
-- | I64(4) | I64(7) | Str("Monday") |
-- | I64(3) | NULL   | Str("World")  |

SELECT * FROM Test ORDER BY id + num ASC;
-- @expect-index: idx_id_num_asc ASC
-- @expect:
-- | id     | num    | name          |
-- | ------ | ------ | ------------- |
-- | I64(1) | I64(2) | Str("Hello")  |
-- | I64(1) | I64(9) | Str("Wild")   |
-- | I64(4) | I64(7) | Str("Monday") |
-- | I64(3) | NULL   | Str("World")  |

SELECT * FROM Test WHERE id < 4 ORDER BY num DESC;
-- @expect-index: idx_num_desc DESC
-- @expect:
-- | id     | num    | name         |
-- | ------ | ------ | ------------ |
-- | I64(3) | NULL   | Str("World") |
-- | I64(1) | I64(9) | Str("Wild")  |
-- | I64(1) | I64(2) | Str("Hello") |
