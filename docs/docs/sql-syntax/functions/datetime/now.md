# NOW

The `NOW()` function in SQL returns the current date and time in UTC. You can use it to retrieve the current UTC timestamp, or as a default value for a TIMESTAMP column in a table. 

## Syntax

```
NOW()
```

## Examples

### Creating a table with a TIMESTAMP column and setting the default value to NOW()

```sql
CREATE TABLE Item (time TIMESTAMP DEFAULT NOW());
```

This creates a table named `Item` with a column `time` of the type TIMESTAMP. The default value for this column is the current UTC timestamp.

### Inserting data into the table

```sql
INSERT INTO Item (time) VALUES
    ('2021-10-13T06:42:40.364832862'),
    ('9999-12-31T23:59:40.364832862');
```

Here we're inserting two rows into the `Item` table with specific timestamps.

### Selecting rows where the timestamp is greater than the current timestamp

```sql
SELECT time FROM Item WHERE time > NOW();
```

This query selects the `time` column from the `Item` table where the `time` is greater than the current UTC timestamp. In this case, the result will be:

```
9999-12-31T23:59:40.364832862
```