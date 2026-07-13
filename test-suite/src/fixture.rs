use {
    crate::{Tester, test_indexes},
    chrono::{NaiveDate, NaiveDateTime, NaiveTime},
    gluesql_core::{
        ast::{DataType, IndexOperator},
        data::{Interval, Point},
        executor::Payload,
        plan::IndexItemPlan,
        prelude::Value,
        store::{GStore, GStoreMut, Planner},
    },
    include_dir::{Dir, include_dir},
    pretty_assertions::assert_eq as pretty_assert_eq,
    rust_decimal::Decimal,
    serde_json::{Map as JsonMap, Value as JsonValue},
    std::{net::IpAddr, str::FromStr},
    uuid::Uuid,
};

static FIXTURES: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/fixtures");

pub fn source(path: &str) -> &'static str {
    FIXTURES
        .get_file(path)
        .unwrap_or_else(|| panic!("fixture not found: {path}"))
        .contents_utf8()
        .unwrap_or_else(|| panic!("fixture must be UTF-8: {path}"))
}

#[derive(Debug, PartialEq)]
enum Expectation {
    Ok,
    Select(Payload),
    Maps(Payload),
    Payload(SerdeExpectation),
    Error(SerdeExpectation),
    Count(usize),
    Types(Vec<DataType>),
}

#[derive(Debug, PartialEq)]
struct SerdeExpectation {
    path: Vec<String>,
    body: Option<JsonValue>,
}

#[derive(Debug, PartialEq)]
struct FixtureStep {
    name: Option<String>,
    sql: String,
    indexes: Option<Vec<IndexItemPlan>>,
    expectation: Expectation,
    line: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Decimal,
    Str,
    Bytea,
    Inet,
    Date,
    Timestamp,
    Time,
    Interval,
    Uuid,
    Map,
    List,
    Point,
}

#[derive(Debug, PartialEq)]
struct Column {
    label: String,
    type_: Option<ColumnType>,
}

pub fn run_fixture<T>(mut tester: impl Tester<T>, fixture_path: &str, source: &str)
where
    T: GStore + GStoreMut + Planner,
{
    let glue = tester.get_glue();

    for step in parse_fixture_with_context(fixture_path, source) {
        let context = step_context(fixture_path, &step);
        let actual = match step.indexes {
            Some(indexes) => {
                let statements = glue.plan(&step.sql).unwrap_or_else(|error| {
                    panic!("{context}\nexpected an index plan but planning failed: {error:#?}")
                });
                let [statement] = statements.as_slice() else {
                    panic!(
                        "{context}\nindex expectation requires one statement, found {}",
                        statements.len()
                    );
                };

                assert_indexes_with_context(statement, indexes, &context);
                glue.execute_stmt(statement).map(|payload| vec![payload])
            }
            None => glue.execute(&step.sql),
        };

        match step.expectation {
            Expectation::Ok => {
                actual.unwrap_or_else(|error| panic!("{context}\nexpected success: {error:#?}"));
            }
            Expectation::Select(expected) | Expectation::Maps(expected) => {
                pretty_assert_eq!(actual, Ok(vec![expected]), "{context}");
            }
            Expectation::Payload(expected) => {
                let payloads = actual.unwrap_or_else(|error| {
                    panic!("{context}\nexpected payload but execution failed: {error:#?}")
                });
                let [payload] = payloads.as_slice() else {
                    panic!("{context}\nexpected one payload, found {}", payloads.len());
                };
                assert_serialized(payload, &expected, &context);
            }
            Expectation::Error(expected) => {
                let error = actual.expect_err(&format!("{context}\nexpected execution to fail"));
                assert_serialized(&error, &expected, &context);
            }
            Expectation::Count(expected) => {
                let payloads = actual.unwrap_or_else(|error| {
                    panic!("{context}\nexpected count payload but execution failed: {error:#?}")
                });
                let [payload] = payloads.as_slice() else {
                    panic!("{context}\nexpected one payload, found {}", payloads.len());
                };
                let actual = match payload {
                    Payload::Select { rows, .. } => rows.len(),
                    Payload::Delete(count) | Payload::Update(count) => *count,
                    _ => panic!("{context}\ncount requires Select, Delete, or Update"),
                };
                pretty_assert_eq!(actual, expected, "{context}");
            }
            Expectation::Types(expected) => {
                let payloads = actual.unwrap_or_else(|error| {
                    panic!("{context}\nexpected Select payload but execution failed: {error:#?}")
                });
                let [payload] = payloads.as_slice() else {
                    panic!("{context}\nexpected one payload, found {}", payloads.len());
                };
                assert_types(payload, &expected, &context);
            }
        }
    }
}

fn assert_indexes_with_context(
    statement: &gluesql_core::plan::StatementPlan,
    indexes: Vec<IndexItemPlan>,
    context: &str,
) {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        test_indexes(statement, Some(indexes));
    }))
    .unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("unknown index assertion panic");
        panic!("{context}\nindex expectation failed: {message}")
    });
}

fn parse_fixture_with_context(fixture_path: &str, source: &str) -> Vec<FixtureStep> {
    std::panic::catch_unwind(|| parse_fixture(source)).unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("unknown fixture parser panic");
        panic!("[FIXTURE] {fixture_path}\nfailed to parse fixture: {message}")
    })
}

fn assert_types(payload: &Payload, expected: &[DataType], context: &str) {
    let Payload::Select { rows, .. } = payload else {
        panic!("{context}\ntypes expectation requires a Select payload")
    };

    for (row_index, row) in rows.iter().enumerate() {
        pretty_assert_eq!(
            row.len(),
            expected.len(),
            "{context}\nrow {row_index} has the wrong number of values"
        );

        for (value, data_type) in row.iter().zip(expected) {
            value.validate_type(data_type).unwrap_or_else(|error| {
                panic!(
                    "{context}\ntype mismatch in row {row_index}: \
                     expected {data_type:?}, found {value:?}: {error:?}"
                )
            });
        }
    }
}

fn step_context(fixture_path: &str, step: &FixtureStep) -> String {
    let name = step
        .name
        .as_deref()
        .map(|name| format!("\n[NAME] {name}"))
        .unwrap_or_default();

    format!(
        "[FIXTURE] {fixture_path}:{}{}\n[SQL]\n{}",
        step.line, name, step.sql
    )
}

fn assert_serialized<T: serde::Serialize>(actual: &T, expected: &SerdeExpectation, context: &str) {
    let actual = serde_json::to_value(actual)
        .unwrap_or_else(|error| panic!("{context}\nfailed to serialize value: {error}"));

    match &expected.body {
        Some(body) => {
            let expected = wrap_json_path(&expected.path, body.clone());
            pretty_assert_eq!(actual, expected, "{context}");
        }
        None => assert!(
            json_has_path(&actual, &expected.path),
            "{context}\nexpected variant path {}, found {actual:#}",
            expected.path.join(".")
        ),
    }
}

fn wrap_json_path(path: &[String], body: JsonValue) -> JsonValue {
    path.iter().rev().fold(body, |value, segment| {
        JsonValue::Object(JsonMap::from_iter([(segment.clone(), value)]))
    })
}

fn json_has_path(value: &JsonValue, path: &[String]) -> bool {
    let mut value = value;

    for (index, segment) in path.iter().enumerate() {
        match value {
            JsonValue::Object(object) => {
                let Some(next) = object.get(segment) else {
                    return false;
                };
                value = next;
            }
            JsonValue::String(found) if index + 1 == path.len() => return found == segment,
            _ => return false,
        }
    }

    true
}

fn parse_fixture(source: &str) -> Vec<FixtureStep> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut steps = Vec::new();
    let mut sql = Vec::new();
    let mut name = None;
    let mut indexes = None;
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index];
        let trimmed = line.trim();

        if let Some(value) = trimmed.strip_prefix("-- name:") {
            assert!(
                sql.iter().all(|line: &&str| line.trim().is_empty()),
                "name must appear before fixture SQL at line {}",
                index + 1
            );
            assert!(
                name.is_none(),
                "duplicate fixture name at line {}",
                index + 1
            );
            let value = value.trim();
            assert!(!value.is_empty(), "fixture name cannot be empty");
            sql.clear();
            name = Some(value.to_owned());
            index += 1;
            continue;
        }

        if let Some(value) = trimmed.strip_prefix("-- expect-index:") {
            assert_follows_sql(&sql, "index expectation", index + 1);

            let value = value.trim();
            assert!(!value.is_empty(), "index expectation cannot be empty");

            if value == "none" {
                assert!(
                    indexes.is_none(),
                    "`none` cannot be combined with another index expectation"
                );
                indexes = Some(Vec::new());
            } else {
                let expected = parse_index(value);
                match &mut indexes {
                    None => indexes = Some(vec![expected]),
                    Some(indexes) if indexes.is_empty() => {
                        panic!("`none` cannot be combined with another index expectation")
                    }
                    Some(indexes) => indexes.push(expected),
                }
            }

            index += 1;
            continue;
        }

        if let Some(directive) = trimmed.strip_prefix("-- expect:") {
            assert_follows_sql(&sql, "result expectation", index + 1);
            let step_line = first_sql_line(&sql, index + 1);
            let sql = take_sql(&mut sql);
            let directive = directive.trim();
            let (expectation, next_index) = parse_expectation(&lines, index + 1, directive);
            steps.push(FixtureStep {
                name: name.take(),
                sql,
                indexes: indexes.take(),
                expectation,
                line: step_line,
            });
            index = next_index;
            continue;
        }

        if name.is_some() && sql.is_empty() {
            assert!(
                !trimmed.is_empty(),
                "fixture name must be immediately followed by SQL at line {}",
                index + 1
            );
        }

        sql.push(line);
        index += 1;
    }

    assert!(
        sql.iter().all(|line| line.trim().is_empty()),
        "fixture SQL must end with an expectation"
    );
    assert!(
        name.is_none(),
        "fixture name must be followed by SQL and an expectation"
    );
    assert!(
        indexes.is_none(),
        "index expectation must be followed by a result expectation"
    );

    steps
}

fn assert_follows_sql(lines: &[&str], expectation: &str, line: usize) {
    assert!(
        lines.last().is_some_and(|line| !line.trim().is_empty()),
        "{expectation} must immediately follow fixture SQL at line {line}"
    );
}

fn parse_index(value: &str) -> IndexItemPlan {
    let mut parts = value.splitn(2, char::is_whitespace);
    let name = parts.next().expect("index expectation requires a name");
    let detail = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let (asc, cmp_expr) = match detail {
        None => (None, None),
        Some("ASC") => (Some(true), None),
        Some("DESC") => (Some(false), None),
        Some(detail) => {
            let mut parts = detail.splitn(2, char::is_whitespace);
            let operator = match parts.next().unwrap() {
                ">" => IndexOperator::Gt,
                "<" => IndexOperator::Lt,
                ">=" => IndexOperator::GtEq,
                "<=" => IndexOperator::LtEq,
                "=" => IndexOperator::Eq,
                operator => panic!("unsupported index operator: {operator}"),
            };
            let expression = parts
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .expect("index comparison requires a SQL expression");
            let expression = gluesql_core::parse_sql::parse_expr(expression)
                .expect("index comparison requires a valid SQL expression");
            let expression = gluesql_core::translate::translate_expr(&expression, &[])
                .expect("index comparison expression must translate");

            (None, Some((operator, expression.into())))
        }
    };

    IndexItemPlan::NonClustered {
        name: name.to_owned(),
        asc,
        cmp_expr,
    }
}

fn first_sql_line(lines: &[&str], expectation_line: usize) -> usize {
    lines
        .iter()
        .position(|line| !line.trim().is_empty())
        .map_or(expectation_line, |position| {
            expectation_line - lines.len() + position
        })
}

fn take_sql(lines: &mut Vec<&str>) -> String {
    let sql = lines.join("\n").trim().to_owned();
    lines.clear();
    assert!(!sql.is_empty(), "expectation must follow fixture SQL");
    sql
}

fn parse_expectation(lines: &[&str], index: usize, directive: &str) -> (Expectation, usize) {
    match directive {
        "" => {
            let (table, index) = take_table_rows(lines, index);
            assert!(
                !table.is_empty(),
                "select expectation requires a header row"
            );
            (Expectation::Select(parse_select(&table)), index)
        }
        "ok" => (Expectation::Ok, index),
        "maps" => {
            let (rows, index) = take_table_rows(lines, index);
            (Expectation::Maps(parse_maps(&rows)), index)
        }
        "types" => {
            let (rows, index) = take_table_rows(lines, index);
            assert_eq!(rows.len(), 1, "types expectation requires one type row");
            let types = parse_table_row(rows[0])
                .into_iter()
                .map(parse_data_type)
                .collect();
            (Expectation::Types(types), index)
        }
        _ if directive.starts_with("count ") => {
            let count = directive["count ".len()..]
                .parse()
                .expect("count expectation requires an unsigned integer");
            (Expectation::Count(count), index)
        }
        _ if directive.starts_with("payload ") => {
            let path = parse_variant_path(&directive["payload ".len()..]);
            let (body, index) = take_json_body(lines, index);
            (Expectation::Payload(SerdeExpectation { path, body }), index)
        }
        _ if directive.starts_with("error ") => {
            let path = parse_variant_path(&directive["error ".len()..]);
            let (body, index) = take_json_body(lines, index);
            (Expectation::Error(SerdeExpectation { path, body }), index)
        }
        _ => panic!("unsupported fixture expectation: {directive}"),
    }
}

fn take_table_rows<'a>(lines: &[&'a str], mut index: usize) -> (Vec<&'a str>, usize) {
    let mut rows = Vec::new();
    while index < lines.len() && lines[index].trim_start().starts_with("-- |") {
        rows.push(lines[index]);
        index += 1;
    }
    assert_table_format(&rows);
    (rows, index)
}

fn assert_table_format(lines: &[&str]) {
    if lines.is_empty() {
        return;
    }
    let rows = lines
        .iter()
        .map(|line| parse_table_row(line))
        .collect::<Vec<_>>();
    let column_count = rows[0].len();

    for row in &rows {
        assert_eq!(
            row.len(),
            column_count,
            "table rows must have the same number of columns"
        );
    }

    let widths = (0..column_count)
        .map(|column| {
            rows.iter()
                .map(|row| row[column].chars().count())
                .max()
                .unwrap()
        })
        .collect::<Vec<_>>();

    for (line, row) in lines.iter().zip(rows) {
        let cells = row
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect::<Vec<_>>()
            .join(" | ");
        let expected = format!("-- | {cells} |");
        assert_eq!(
            *line, expected,
            "table rows must use canonical spacing and padding"
        );
    }
}

fn take_json_body(lines: &[&str], mut index: usize) -> (Option<JsonValue>, usize) {
    let mut body = Vec::new();

    while index < lines.len() {
        let trimmed = lines[index].trim_start();
        if trimmed.starts_with("-- name:") || trimmed.starts_with("-- expect:") {
            break;
        }
        let Some(comment) = trimmed.strip_prefix("--") else {
            break;
        };
        body.push(comment.strip_prefix(' ').unwrap_or(comment));
        index += 1;
    }

    if body.is_empty() {
        (None, index)
    } else {
        let value = serde_json::from_str(&body.join("\n"))
            .expect("payload or error expectation requires valid JSON");
        (Some(value), index)
    }
}

fn parse_variant_path(path: &str) -> Vec<String> {
    let path = path.trim();
    assert!(!path.is_empty(), "variant path cannot be empty");
    path.split('.')
        .map(|segment| {
            assert!(
                !segment.is_empty()
                    && segment
                        .chars()
                        .all(|character| character == '_' || character.is_ascii_alphanumeric()),
                "invalid variant path: {path}"
            );
            segment.to_owned()
        })
        .collect()
}

fn parse_select(lines: &[&str]) -> Payload {
    let columns = parse_table_row(lines[0])
        .into_iter()
        .map(parse_column)
        .collect::<Vec<_>>();
    let labels = columns.iter().map(|column| column.label.clone()).collect();
    let rows = lines[1..]
        .iter()
        .map(|line| {
            let cells = parse_table_row(line);
            pretty_assert_eq!(
                cells.len(),
                columns.len(),
                "result row must match the number of columns"
            );
            cells
                .into_iter()
                .zip(&columns)
                .map(|(cell, column)| parse_value(cell, column.type_))
                .collect()
        })
        .collect();

    Payload::Select { labels, rows }
}

fn parse_maps(lines: &[&str]) -> Payload {
    let rows = lines
        .iter()
        .map(|line| {
            let row = parse_single_table_cell(line);
            let Value::Map(map) = Value::parse_json_map(row).expect("expected a JSON map") else {
                unreachable!()
            };
            map
        })
        .collect();

    Payload::SelectMap(rows)
}

fn parse_table_row(line: &str) -> Vec<&str> {
    parse_single_table_cell(line)
        .split('|')
        .map(str::trim)
        .collect()
}

fn parse_single_table_cell(line: &str) -> &str {
    line.trim()
        .strip_prefix("-- |")
        .and_then(|row| row.strip_suffix('|'))
        .map(str::trim)
        .expect("table row should use `-- | ... |`")
}

fn parse_column(column: &str) -> Column {
    let (label, type_) = column
        .rsplit_once(": ")
        .and_then(|(label, type_)| parse_column_type(type_).map(|type_| (label, type_)))
        .map_or_else(
            || (parse_label(column), None),
            |(label, type_)| (parse_label(label), Some(type_)),
        );

    Column { label, type_ }
}

fn parse_label(label: &str) -> String {
    if label.starts_with('"') {
        parse_string(label)
    } else {
        label.to_owned()
    }
}

fn parse_column_type(type_: &str) -> Option<ColumnType> {
    Some(match type_ {
        "Bool" => ColumnType::Bool,
        "I8" => ColumnType::I8,
        "I16" => ColumnType::I16,
        "I32" => ColumnType::I32,
        "I64" => ColumnType::I64,
        "I128" => ColumnType::I128,
        "U8" => ColumnType::U8,
        "U16" => ColumnType::U16,
        "U32" => ColumnType::U32,
        "U64" => ColumnType::U64,
        "U128" => ColumnType::U128,
        "F32" => ColumnType::F32,
        "F64" => ColumnType::F64,
        "Decimal" => ColumnType::Decimal,
        "Str" => ColumnType::Str,
        "Bytea" => ColumnType::Bytea,
        "Inet" => ColumnType::Inet,
        "Date" => ColumnType::Date,
        "Timestamp" => ColumnType::Timestamp,
        "Time" => ColumnType::Time,
        "Interval" => ColumnType::Interval,
        "Uuid" => ColumnType::Uuid,
        "Map" => ColumnType::Map,
        "List" => ColumnType::List,
        "Point" => ColumnType::Point,
        _ => return None,
    })
}

fn parse_value(value: &str, type_: Option<ColumnType>) -> Value {
    if value == "NULL" {
        return Value::Null;
    }

    if let Some(type_) = type_ {
        parse_typed_value(value, type_)
    } else {
        let (type_, value) = value
            .split_once('(')
            .and_then(|(type_, value)| value.strip_suffix(')').map(|value| (type_, value)))
            .expect("dynamic value should use `Type(value)`");
        let type_ = parse_column_type(type_).expect("unsupported dynamic value type");
        parse_typed_value(value.trim(), type_)
    }
}

fn parse_typed_value(value: &str, type_: ColumnType) -> Value {
    match type_ {
        ColumnType::Bool => Value::Bool(value.parse().expect("expected a Bool value")),
        ColumnType::I8 => Value::I8(value.parse().expect("expected an I8 value")),
        ColumnType::I16 => Value::I16(value.parse().expect("expected an I16 value")),
        ColumnType::I32 => Value::I32(value.parse().expect("expected an I32 value")),
        ColumnType::I64 => Value::I64(value.parse().expect("expected an I64 value")),
        ColumnType::I128 => Value::I128(value.parse().expect("expected an I128 value")),
        ColumnType::U8 => Value::U8(value.parse().expect("expected a U8 value")),
        ColumnType::U16 => Value::U16(value.parse().expect("expected a U16 value")),
        ColumnType::U32 => Value::U32(value.parse().expect("expected a U32 value")),
        ColumnType::U64 => Value::U64(value.parse().expect("expected a U64 value")),
        ColumnType::U128 => Value::U128(value.parse().expect("expected a U128 value")),
        ColumnType::F32 => Value::F32(value.parse().expect("expected an F32 value")),
        ColumnType::F64 => Value::F64(value.parse().expect("expected an F64 value")),
        ColumnType::Decimal => {
            Value::Decimal(Decimal::from_str(unquote(value)).expect("expected a Decimal value"))
        }
        ColumnType::Str => Value::Str(parse_string(value)),
        ColumnType::Bytea => {
            Value::Bytea(hex::decode(unquote(value)).expect("expected hexadecimal Bytea"))
        }
        ColumnType::Inet => Value::Inet(
            unquote(value)
                .parse::<IpAddr>()
                .expect("expected an Inet value"),
        ),
        ColumnType::Date => Value::Date(
            unquote(value)
                .parse::<NaiveDate>()
                .expect("expected a Date value"),
        ),
        ColumnType::Timestamp => Value::Timestamp(
            NaiveDateTime::parse_from_str(unquote(value), "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| unquote(value).parse::<NaiveDateTime>())
                .expect("expected a Timestamp value"),
        ),
        ColumnType::Time => Value::Time(
            unquote(value)
                .parse::<NaiveTime>()
                .expect("expected a Time value"),
        ),
        ColumnType::Interval => Value::Interval(
            Interval::parse(unquote(value)).expect("expected an Interval SQL literal"),
        ),
        ColumnType::Uuid => Value::Uuid(
            Uuid::parse_str(unquote(value))
                .expect("expected a UUID value")
                .as_u128(),
        ),
        ColumnType::Map => Value::parse_json_map(value).expect("expected a JSON Map value"),
        ColumnType::List => Value::parse_json_list(value).expect("expected a JSON List value"),
        ColumnType::Point => {
            Value::Point(Point::from_wkt(unquote(value)).expect("expected a Point WKT value"))
        }
    }
}

fn parse_string(value: &str) -> String {
    serde_json::from_str(value).expect("expected a JSON quoted string")
}

fn unquote(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

fn parse_data_type(value: &str) -> DataType {
    match value {
        "Boolean" => DataType::Boolean,
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int" => DataType::Int,
        "Int128" => DataType::Int128,
        "Uint8" => DataType::Uint8,
        "Uint16" => DataType::Uint16,
        "Uint32" => DataType::Uint32,
        "Uint64" => DataType::Uint64,
        "Uint128" => DataType::Uint128,
        "Float32" => DataType::Float32,
        "Float" => DataType::Float,
        "Text" => DataType::Text,
        "Bytea" => DataType::Bytea,
        "Inet" => DataType::Inet,
        "Date" => DataType::Date,
        "Timestamp" => DataType::Timestamp,
        "Time" => DataType::Time,
        "Interval" => DataType::Interval,
        "Uuid" => DataType::Uuid,
        "Map" => DataType::Map,
        "List" => DataType::List,
        "Decimal" => DataType::Decimal,
        "Point" => DataType::Point,
        _ => panic!("unsupported DataType: {value}"),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::stringify_label,
        gluesql_core::{
            error::{EvaluateError, TranslateError},
            prelude::Error,
        },
    };

    #[test]
    fn parses_named_steps_and_all_expectation_kinds() {
        let source = r#"
CREATE TABLE Example (id INTEGER);
-- expect: ok

-- name: mixed result
SELECT 1;
-- expect:
-- | fixed: I64 | dynamic  |
-- | 1          | Str("a") |
-- | NULL       | I64(2)   |

UPDATE Example SET id = 1;
-- expect: payload Update
-- 0

SELECT * FROM Example;
-- expect: count 0

SELECT GENERATE_UUID();
-- expect: types
-- | Uuid |

SELECT NULLIF();
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "name": "NULLIF",
--   "expected": 2,
--   "found": 0
-- }

SELECT ADD_MONTH('invalid', 1);
-- expect: error Evaluate.FormatParseError
"#;

        let steps = parse_fixture(source);
        assert_eq!(steps.len(), 7);
        assert_eq!(steps[1].name.as_deref(), Some("mixed result"));
        assert_eq!(steps[2].name, None);
        assert!(matches!(steps[0].expectation, Expectation::Ok));
        assert!(matches!(steps[1].expectation, Expectation::Select(_)));
        assert!(matches!(steps[2].expectation, Expectation::Payload(_)));
        assert!(matches!(steps[3].expectation, Expectation::Count(0)));
        assert!(matches!(steps[4].expectation, Expectation::Types(_)));
        assert!(matches!(steps[5].expectation, Expectation::Error(_)));
        assert!(matches!(steps[6].expectation, Expectation::Error(_)));
    }

    #[test]
    fn parses_every_value_variant() {
        let source = r#"
SELECT 1;
-- expect:
-- | bool: Bool | i8: I8 | i16: I16 | i32: I32 | i64: I64 | i128: I128 | u8: U8 | u16: U16 | u32: U32 | u64: U64 | u128: U128 | f32: F32 | f64: F64 | decimal: Decimal | str: Str | bytea: Bytea | inet: Inet  | date: Date   | timestamp: Timestamp  | time: Time | interval: Interval | uuid: Uuid                             | map: Map | list: List | point: Point |
-- | true       | -8     | -16      | -32      | -64      | -128       | 8      | 16       | 32       | 64       | 128        | 1.5      | 2.5      | 3.14             | "text"   | "00ff"       | "127.0.0.1" | "2025-01-02" | "2025-01-02T03:04:05" | "03:04:05" | "'1' DAY"          | "550e8400-e29b-41d4-a716-446655440000" | {"a":1}  | [1,"a"]    | "POINT(1 2)" |
"#;

        let steps = parse_fixture(source);
        let Expectation::Select(Payload::Select { rows, .. }) = &steps[0].expectation else {
            panic!("expected Select")
        };
        assert_eq!(rows[0].len(), 25);
    }

    #[test]
    fn compares_exact_and_variant_only_errors() {
        let exact = Error::Translate(TranslateError::FunctionArgsLengthNotMatching {
            name: "NULLIF".to_owned(),
            expected: 2,
            found: 0,
        });
        let exact_expected = SerdeExpectation {
            path: vec![
                "Translate".to_owned(),
                "FunctionArgsLengthNotMatching".to_owned(),
            ],
            body: Some(serde_json::json!({
                "name": "NULLIF",
                "expected": 2,
                "found": 0,
            })),
        };
        assert_serialized(&exact, &exact_expected, "exact error");

        let variant = Error::Evaluate(EvaluateError::FormatParseError("details".to_owned()));
        let variant_expected = SerdeExpectation {
            path: vec!["Evaluate".to_owned(), "FormatParseError".to_owned()],
            body: None,
        };
        assert_serialized(&variant, &variant_expected, "variant error");
    }

    #[test]
    fn compares_unit_newtype_tuple_and_struct_errors() {
        let cases = [
            (
                Error::Translate(TranslateError::TooManyTables),
                SerdeExpectation {
                    path: vec!["Translate".to_owned(), "TooManyTables".to_owned()],
                    body: None,
                },
            ),
            (
                Error::Evaluate(EvaluateError::FormatParseError("details".to_owned())),
                SerdeExpectation {
                    path: vec!["Evaluate".to_owned(), "FormatParseError".to_owned()],
                    body: Some(serde_json::json!("details")),
                },
            ),
            (
                Error::Evaluate(EvaluateError::IncompatibleBitOperation(
                    "left".to_owned(),
                    "right".to_owned(),
                )),
                SerdeExpectation {
                    path: vec!["Evaluate".to_owned(), "IncompatibleBitOperation".to_owned()],
                    body: Some(serde_json::json!(["left", "right"])),
                },
            ),
            (
                Error::Translate(TranslateError::FunctionArgsLengthNotMatching {
                    name: "NULLIF".to_owned(),
                    expected: 2,
                    found: 0,
                }),
                SerdeExpectation {
                    path: vec![
                        "Translate".to_owned(),
                        "FunctionArgsLengthNotMatching".to_owned(),
                    ],
                    body: Some(serde_json::json!({
                        "name": "NULLIF",
                        "expected": 2,
                        "found": 0,
                    })),
                },
            ),
        ];

        for (actual, expected) in cases {
            assert_serialized(&actual, &expected, "error shape");
        }
    }

    #[test]
    fn parses_select_maps() {
        let source = r#"
SELECT * FROM Example;
-- expect: maps
-- | {"id":1,"name":"one"} |
-- | {"id":2}              |
"#;

        let steps = parse_fixture(source);
        let Expectation::Maps(Payload::SelectMap(rows)) = &steps[0].expectation else {
            panic!("expected SelectMap")
        };
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn parses_empty_select_maps() {
        let source = r"
SELECT * FROM Example;
-- expect: maps
";

        let steps = parse_fixture(source);
        let Expectation::Maps(Payload::SelectMap(rows)) = &steps[0].expectation else {
            panic!("expected SelectMap")
        };
        assert!(rows.is_empty());
    }

    #[test]
    fn parses_index_expectations() {
        let source = r"
SELECT * FROM Example WHERE id < 20;
-- expect-index: idx_id < 20
-- expect: ok

SELECT * FROM Example ORDER BY id;
-- expect-index: idx_id ASC
-- expect: ok

SELECT * FROM Example ORDER BY name;
-- expect-index: idx_name
-- expect: ok

SELECT * FROM Example;
-- expect-index: none
-- expect: ok
";

        let steps = parse_fixture(source);
        assert_eq!(
            steps[0].indexes,
            Some(crate::idx!(idx_id, IndexOperator::Lt, "20"))
        );
        assert_eq!(steps[1].indexes, Some(crate::idx!(idx_id, ASC)));
        assert_eq!(steps[2].indexes, Some(crate::idx!(idx_name)));
        assert_eq!(steps[3].indexes, Some(crate::idx!()));
    }

    #[test]
    fn rejects_missing_fixture_and_malformed_sources() {
        assert!(std::panic::catch_unwind(|| source("missing.sql")).is_err());

        let malformed = [
            "SELECT 1;\n-- expect: payload Select\n-- {not json}\n",
            "-- name: first\n-- name: second\nSELECT 1;\n-- expect: ok\n",
            "SELECT 1;\n",
            "-- expect: ok\n",
            "SELECT 1;\n-- expect:\n",
            "SELECT 1;\n-- expect-index: none\n-- expect-index: idx_id\n-- expect: ok\n",
            "SELECT 1;\n-- expect-index: idx_id =\n-- expect: ok\n",
        ];

        for source in malformed {
            assert!(std::panic::catch_unwind(|| parse_fixture(source)).is_err());
        }
    }

    #[test]
    fn rejects_detached_names_and_expectations() {
        let detached = [
            "-- name: detached\n\nSELECT 1;\n-- expect: ok\n",
            "SELECT 1;\n\n-- expect: ok\n",
            "SELECT 1;\n-- expect-index: none\n\n-- expect: ok\n",
        ];

        for source in detached {
            assert!(std::panic::catch_unwind(|| parse_fixture(source)).is_err());
        }
    }

    #[test]
    fn rejects_non_canonical_table_spacing() {
        let malformed = [
            "SELECT 1;\n-- expect:\n-- |id: I64|\n-- |1|\n",
            "SELECT 1;\n-- expect:\n-- | id: I64 | value: Str |\n-- | 1 | \"a\" |\n",
        ];

        for source in malformed {
            assert!(std::panic::catch_unwind(|| parse_fixture(source)).is_err());
        }
    }
}
