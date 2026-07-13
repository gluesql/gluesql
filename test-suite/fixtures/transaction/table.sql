BEGIN;
-- expect: ok

CREATE TABLE Test (id INTEGER);
-- expect: ok

INSERT INTO Test VALUES (1);
-- expect: ok

SELECT * FROM Test;
-- expect:
-- | id: I64 |
-- | 1       |

ROLLBACK;
-- expect: ok

SELECT * FROM Test;
-- expect: error Fetch.TableNotFound
-- "Test"

BEGIN;
-- expect: ok

CREATE TABLE Test (id INTEGER);
-- expect: ok

INSERT INTO Test VALUES (3);
-- expect: ok

COMMIT;
-- expect: ok

SELECT * FROM Test;
-- expect:
-- | id: I64 |
-- | 3       |

BEGIN;
-- expect: ok

DROP TABLE Test;
-- expect: ok

SELECT * FROM Test;
-- expect: error Fetch.TableNotFound
-- "Test"

ROLLBACK;
-- expect: ok

SELECT * FROM Test;
-- expect:
-- | id: I64 |
-- | 3       |

BEGIN;
-- expect: ok

DROP TABLE Test;
-- expect: ok

COMMIT;
-- expect: ok

SELECT * FROM Test;
-- expect: error Fetch.TableNotFound
-- "Test"
