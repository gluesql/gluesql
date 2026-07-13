SELECT generate_uuid(0) as uuid
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 0,
--   "found": 1,
--   "name": "GENERATE_UUID"
-- }

SELECT GENERATE_UUID()
-- expect: count 1

VALUES (GENERATE_UUID())
-- expect: count 1

SELECT GENERATE_UUID() as uuid
-- expect: types
-- | Uuid |

VALUES (GENERATE_UUID())
-- expect: types
-- | Uuid |
