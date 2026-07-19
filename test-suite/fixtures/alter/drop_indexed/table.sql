DROP TABLE IF EXISTS Test;
-- @expect: ok

CREATE TABLE Test (id INTEGER);
-- @expect: ok

INSERT INTO Test VALUES (1), (2);
-- @expect: ok

CREATE INDEX idx_id ON Test (id);
-- @expect: ok

SELECT * FROM Test WHERE id = 1;
-- @expect-index: idx_id = 1
-- @expect:
-- | id: I64 |
-- | 1       |

DROP TABLE Test;
-- @expect: ok

SELECT * FROM Test;
-- @expect: error Fetch.TableNotFound
-- @json: "Test"

CREATE TABLE Test (id INTEGER);
-- @expect: ok

INSERT INTO Test VALUES (3), (4);
-- @expect: ok

SELECT * FROM Test WHERE id = 3;
-- @expect-index: none
-- @expect:
-- | id: I64 |
-- | 3       |

CREATE INDEX idx_id ON Test (id);
-- @expect: ok

SELECT * FROM Test WHERE id < 10;
-- @expect-index: idx_id < 10
-- @expect:
-- | id: I64 |
-- | 3       |
-- | 4       |

DROP INDEX Test;
-- @expect: error Translate.InvalidParamsInDropIndex

DROP INDEX Test.idx_id.IndexC;
-- @expect: error Translate.InvalidParamsInDropIndex
