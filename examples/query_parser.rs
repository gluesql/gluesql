use gluesql::prelude::{parse, translate};

fn main() {
    let sql = "SELECT * FROM (SELECT * FROM F) AS F";

    let parsed = parse(sql).unwrap().into_iter().next().unwrap();
    let statement = translate(&parsed).unwrap();

    println!("{statement:#?}");
}
