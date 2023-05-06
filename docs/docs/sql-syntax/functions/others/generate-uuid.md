# GENERATE_UUID

The `GENERATE_UUID` function is an SQL function provided by GlueSQL that generates a new UUID (Universally Unique Identifier) using the version 4 UUID algorithm. A UUID is a 128-bit value used to uniquely identify items in various computing systems. Version 4 UUIDs are randomly generated and have 122 bits of randomness, which ensures a very low probability of collisions.

## Syntax

```sql
GENERATE_UUID()
```

## Usage

### Creating a table with a UUID column

You can use the `GENERATE_UUID` function as the default value for a UUID column in a table.

```sql
CREATE TABLE SingleItem (id UUID DEFAULT GENERATE_UUID());
```

This SQL statement creates a table called `SingleItem` with a column named `id` of data type `UUID`. The default value for the `id` column is generated using the `GENERATE_UUID` function.

### Inserting data with a UUID column

You can also use the `GENERATE_UUID` function directly when inserting data into a table.

```sql
INSERT INTO SingleItem VALUES (GENERATE_UUID());
```

This SQL statement inserts a new row into the `SingleItem` table with a UUID value generated using the `GENERATE_UUID` function.

### Selecting data with a UUID column

You can use the `GENERATE_UUID` function in a SELECT statement to generate UUIDs on the fly.

```sql
SELECT GENERATE_UUID() as uuid FROM SingleItem;
```

This SQL statement selects a new UUID for each row in the `SingleItem` table.

## Error Handling

The `GENERATE_UUID` function does not accept any arguments. If you provide any arguments to the function, an error will be raised.

```sql
SELECT generate_uuid(0) as uuid FROM SingleItem;
```

This SQL statement will result in an error, as the `GENERATE_UUID` function does not accept any arguments.
