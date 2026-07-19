CREATE TABLE posts (id UUID PRIMARY KEY)
-- @expect: payload Create

INSERT INTO posts ("id") VALUES ('019f576b-437e-7fe1-bf9d-f9b30f69fae1')
-- @expect: payload Insert
-- @json: 1

SELECT id FROM posts WHERE id = '019f576b-437e-7fe1-bf9d-f9b30f69fae1';
-- @expect:
-- | id: Uuid                               |
-- | "019f576b-437e-7fe1-bf9d-f9b30f69fae1" |

CREATE TABLE UUID (uuid_field UUID)
-- @expect: payload Create

INSERT INTO UUID VALUES (0)
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Uuid",
--   "literal": "0"
-- }

INSERT INTO UUID VALUES (X'1234')
-- @expect: error Value.FailedToParseUUID
-- @json: "1234"

INSERT INTO UUID VALUES ('NOT_UUID')
-- @expect: error Value.FailedToParseUUID
-- @json: "NOT_UUID"

INSERT INTO UUID VALUES
    (X'936DA01F9ABD4d9d80C702AF85C822A8'),
    ('550e8400-e29b-41d4-a716-446655440000'),
    ('urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4')
-- @expect: payload Insert
-- @json: 3

SELECT uuid_field AS uuid_field FROM UUID;
-- @expect:
-- | uuid_field: Uuid                       |
-- | "936da01f-9abd-4d9d-80c7-02af85c822a8" |
-- | "550e8400-e29b-41d4-a716-446655440000" |
-- | "f9168c5e-ceb2-4faa-b6bf-329bf39fa1e4" |

UPDATE UUID SET uuid_field = 'urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4' WHERE uuid_field='550e8400-e29b-41d4-a716-446655440000'
-- @expect: payload Update
-- @json: 1

SELECT uuid_field AS uuid_field, COUNT(*) FROM UUID GROUP BY uuid_field
-- @expect:
-- | uuid_field: Uuid                       | COUNT(*): I64 |
-- | "936da01f-9abd-4d9d-80c7-02af85c822a8" | 1             |
-- | "f9168c5e-ceb2-4faa-b6bf-329bf39fa1e4" | 2             |

DELETE FROM UUID WHERE uuid_field='550e8400-e29b-41d4-a716-446655440000'
-- @expect: payload Delete
-- @json: 0

DELETE FROM UUID WHERE uuid_field='urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4'
-- @expect: payload Delete
-- @json: 2
