# UUID

The `UUID` data type in SQL is used to store universally unique identifiers (UUIDs). These identifiers can be compared, filtered, and sorted using standard SQL operations.

Here's an example of how to create a table, insert data, and query data using the `UUID` data type:

## Creating a table with a UUID column

To create a table with a UUID column, use the following SQL syntax:

```sql
CREATE TABLE UUID (uuid_field UUID);
```

## Inserting data into the UUID column

To insert data into the UUID column, provide the UUIDs as strings or in the format `X'<hexadecimal_value>'`. You can also use the `urn:uuid:` prefix. Note that providing a UUID as a plain number is not supported and will result in an error.

```sql
INSERT INTO UUID VALUES
    (X'936DA01F9ABD4d9d80C702AF85C822A8'),
    ('550e8400-e29b-41d4-a716-446655440000'),
    ('urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4');
```

## Generating a random UUID

To generate a random UUID (version 4), use the `GENERATE_UUID` function:

```sql
INSERT INTO UUID (uuid_field) VALUES (GENERATE_UUID());
```

## Querying data from the UUID column

To query data from the UUID column, use standard SQL syntax:

```sql
SELECT uuid_field FROM UUID;
```

This query will return the following result:

```
uuid_field
------------------------------------
936DA01F-9ABD-4D9D-80C7-02AF85C822A8
550E8400-E29B-41D4-A716-446655440000
F9168C5E-CEB2-4FAA-B6BF-329BF39FA1E4
```

## Filtering and manipulating data using the UUID column

You can filter, update, and delete data using the UUID column with standard SQL operations:

```sql
-- Filtering
SELECT uuid_field FROM UUID WHERE uuid_field = '550e8400-e29b-41d4-a716-446655440000';

-- Updating
UPDATE UUID SET uuid_field = 'urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4' WHERE uuid_field = '550e8400-e29b-41d4-a716-446655440000';

-- Deleting
DELETE FROM UUID WHERE uuid_field = '550e8400-e29b-41d4-a716-446655440000';
```