CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
);
-- expect: ok

INSERT INTO Test (id, num, name)
VALUES
    (1, 2, 'Hello'),
    (1, 17, 'World'),
    (11, 7, 'Great'),
    (4, 7, 'Job');
-- expect: ok

CREATE INDEX idx_id ON Test (id);
-- expect: payload CreateIndex

CREATE INDEX idx_name ON Test (name);
-- expect: payload CreateIndex

CREATE INDEX idx_id2 ON Test (id + num);
-- expect: payload CreateIndex

SHOW INDEXES FROM Test;
-- expect:
-- | TABLE_NAME: Str | INDEX_NAME: Str | ORDER: Str | EXPRESSION: Str | UNIQUENESS: Bool |
-- | "Test"          | "idx_id"        | "BOTH"     | "id"            | false            |
-- | "Test"          | "idx_name"      | "BOTH"     | "name"          | false            |
-- | "Test"          | "idx_id2"       | "BOTH"     | "id + num"      | false            |

SHOW INDEXES FROM NoTable;
-- expect: error Execute.TableNotFound
-- "NoTable"
