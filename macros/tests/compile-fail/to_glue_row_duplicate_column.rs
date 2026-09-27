use gluesql_macros::ToGlueRow;

#[derive(ToGlueRow)]
struct T {
    value: i64,
    #[glue(rename = "value")]
    previous_value: i64,
}
