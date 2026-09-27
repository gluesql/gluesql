CREATE TABLE TableB (id BOOLEAN);
-- @expect: ok

CREATE TABLE TableC (uid INTEGER NOT NULL, null_val INTEGER NULL);
-- @expect: ok

INSERT INTO TableB VALUES (FALSE);
-- @expect: ok

INSERT INTO TableC VALUES (1, NULL);
-- @expect: ok

INSERT INTO TableB SELECT uid FROM TableC;
-- @expect: error Value.IncompatibleDataType
-- @json:
-- {
--   "data_type": "Boolean",
--   "value": {
--     "I64": 1
--   }
-- }

INSERT INTO TableC (uid) VALUES ('A')
-- @expect: error Evaluate.TextParseFailed
-- @json:
-- {
--   "data_type": "Int",
--   "literal": "A"
-- }

INSERT INTO TableC VALUES (NULL, 30);
-- @expect: error Value.NullValueOnNotNullField

INSERT INTO TableC SELECT null_val FROM TableC;
-- @expect: error Value.NullValueOnNotNullField

UPDATE TableC SET uid = TRUE;
-- @expect: error Value.IncompatibleDataType
-- @json:
-- {
--   "data_type": "Int",
--   "value": {
--     "Bool": true
--   }
-- }

UPDATE TableC SET uid = (SELECT id FROM TableB LIMIT 1) WHERE uid = 1
-- @expect: error Value.IncompatibleDataType
-- @json:
-- {
--   "data_type": "Int",
--   "value": {
--     "Bool": false
--   }
-- }

UPDATE TableC SET uid = NULL;
-- @expect: error Value.NullValueOnNotNullField

UPDATE TableC SET uid = (SELECT null_val FROM TableC);
-- @expect: error Value.NullValueOnNotNullField
