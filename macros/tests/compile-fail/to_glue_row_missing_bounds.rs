use {
    gluesql_core::row_conversion::ToGlueRow as _,
    gluesql_macros::ToGlueRow,
};

struct NotConvertible;

#[derive(ToGlueRow)]
struct Record<T> {
    value: T,
}

fn main() {
    let rec = Record {
        value: NotConvertible,
    };
    let _ = rec.to_glue_row();
}
