CREATE TABLE Item
-- @expect: ok

INSERT INTO Item VALUES ('{"dex":324,"id":100,"name":"Test 001","obj":{"cost":3000},"rare":false}');
-- @expect: ok

CREATE TABLE Player
-- @expect: ok

INSERT INTO Player VALUES ('{"flag":1,"id":1001,"name":"Beam"}'), ('{"id":1002,"name":"Seo"}');
-- @expect: ok

CREATE TABLE Food
-- @expect: ok

INSERT INTO Food VALUES (SUBSTR(SUBSTR(' hi{"id":1,"name":"meat","weight":10}', 4), 1));
-- @expect: ok

INSERT INTO Item
VALUES (
    '{ "a": 10 }',
    '{ "b": true }'
);
-- @expect: error Insert.OnlySingleValueAcceptedForSchemalessRow
-- @json: 2

INSERT INTO Item SELECT id, name FROM Item LIMIT 1
-- @expect: error Insert.OnlySingleValueAcceptedForSchemalessRow
-- @json: 2

INSERT INTO Item VALUES ('[1, 2, 3]');
-- @expect: error Value.JsonObjectTypeRequired

INSERT INTO Item VALUES (true);
-- @expect: error Evaluate.MapOrStringValueRequired
-- @json: "TRUE"

INSERT INTO Item VALUES (CAST(1 AS INTEGER) + 4)
-- @expect: error Evaluate.MapOrStringValueRequired
-- @json: "5"

INSERT INTO Item SELECT id FROM Item LIMIT 1
-- @expect: error Insert.MapTypeValueRequired
-- @json: "100"

SELECT id FROM Item WHERE id IN (SELECT * FROM Item)
-- @expect: error Evaluate.SchemalessProjectionForInSubQuery

SELECT id FROM Item WHERE id IN (SELECT * FROM Item AS I)
-- @expect: error Evaluate.SchemalessProjectionForInSubQuery

SELECT id FROM Item WHERE id = (SELECT * FROM Item LIMIT 1)
-- @expect: error Evaluate.SchemalessProjectionForSubQuery

SELECT id FROM Item WHERE id = (SELECT * FROM Item AS I LIMIT 1)
-- @expect: error Evaluate.SchemalessProjectionForSubQuery
