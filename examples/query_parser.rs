use gluesql::prelude::{parse, translate};

fn main() {
    let sql = "SELECT * FROM FOO AS F group by name having name = 'glue'";

    let parsed = parse(sql).unwrap().into_iter().next().unwrap();
    let statement = translate(&parsed).unwrap();

    println!("{statement:#?}");
}
