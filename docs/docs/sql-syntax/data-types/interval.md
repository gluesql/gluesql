---
sidebar_position: 9
---

# INTERVAL

The `INTERVAL` data type in GlueSQL is used to represent a period of time. In accordance with the ANSI SQL standard, several subtypes of `INTERVAL` can be used to represent different units of time, such as years, months, days, hours, minutes, and seconds. These subtypes are:

- YEAR
- YEAR TO MONTH
- MONTH
- DAY
- DAY TO HOUR
- DAY TO MINUTE
- DAY TO SECOND
- HOUR
- HOUR TO MINUTE
- HOUR TO SECOND
- MINUTE
- MINUTE TO SECOND
- SECOND

## Creating a Table with INTERVAL Columns

To create a table with `INTERVAL` columns, simply use the `INTERVAL` keyword for the data type:

```sql
CREATE TABLE IntervalLog (
    id INTEGER,
    interval1 INTERVAL,
    interval2 INTERVAL
);
```

## Inserting INTERVAL Values

To insert `INTERVAL` values into a table, use the `INTERVAL` keyword followed by a string literal representing the interval value:

```sql
INSERT INTO IntervalLog VALUES
    (1, INTERVAL '1-2' YEAR TO MONTH,         INTERVAL 30 MONTH),
    (2, INTERVAL 12 DAY,                      INTERVAL '35' HOUR),
    (3, INTERVAL '12' MINUTE,                 INTERVAL 300 SECOND),
    (4, INTERVAL '-3 14' DAY TO HOUR,         INTERVAL '3 12:30' DAY TO MINUTE),
    (5, INTERVAL '3 14:00:00' DAY TO SECOND,  INTERVAL '3 12:30:12.1324' DAY TO SECOND),
    (6, INTERVAL '12:00' HOUR TO MINUTE,      INTERVAL '-12:30:12' HOUR TO SECOND),
    (7, INTERVAL '-1000-11' YEAR TO MONTH,    INTERVAL '-30:11' MINUTE TO SECOND);
```

## INTERVAL Subtypes and Syntax

Here are some examples of how to use different `INTERVAL` subtypes:

- YEAR: `INTERVAL '5' YEAR`
- YEAR TO MONTH: `INTERVAL '5-3' YEAR TO MONTH`
- MONTH: `INTERVAL '6' MONTH`
- DAY: `INTERVAL '7' DAY`
- DAY TO HOUR: `INTERVAL '2 12' DAY TO HOUR`
- DAY TO MINUTE: `INTERVAL '2 12:30' DAY TO MINUTE`
- DAY TO SECOND: `INTERVAL '2 12:30:45' DAY TO SECOND`
- HOUR: `INTERVAL '18' HOUR`
- HOUR TO MINUTE: `INTERVAL '18:30' HOUR TO MINUTE`
- HOUR TO SECOND: `INTERVAL '18:30:45' HOUR TO SECOND`
- MINUTE: `INTERVAL '45' MINUTE`
- MINUTE TO SECOND: `INTERVAL '45:30' MINUTE TO SECOND`
- SECOND: `INTERVAL '30' SECOND`

## Unsupported Conversions

In GlueSQL, you cannot convert between different `INTERVAL` subtypes, such as converting 1 MONTH to DAYS or converting YEAR TO MONTH to DAY TO SECOND. These conversions are not supported.

## Conclusion

The `INTERVAL` data type is a powerful way to represent time periods in GlueSQL. By following the ANSI SQL standard, you can use a combination of subtypes to represent complex periods of time. Use the `INTERVAL` keyword when creating tables and inserting values to make the most of this data type.