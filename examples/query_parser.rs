use gluesql::prelude::{parse, translate};

fn main() {
    let sql = "SELECT *
    FROM Player
    LEFT JOIN PlayerItem ON
        PlayerItem.amount > 10 AND
        PlayerItem.amount * 3 <= 2 AND
        PlayerItem.user_id = Player.id
    WHERE True;";

    let parsed = parse(sql).unwrap().into_iter().next().unwrap();
    let statement = translate(&parsed).unwrap();

    println!("{statement:#?}");
}
