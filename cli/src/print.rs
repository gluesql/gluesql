use {
    comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_BORDERS_ONLY, Row, Table},
    gluesql::prelude::{Payload, PayloadVariable},
};

pub fn print_payload(payload: Payload) {
    match payload {
        Payload::Insert(n) => print_affected(n, "inserted"),
        Payload::Delete(n) => print_affected(n, "deleted"),
        Payload::Update(n) => print_affected(n, "updated"),
        Payload::ShowVariable(PayloadVariable::Version(v)) => println!("v{}\n", v),
        Payload::ShowVariable(PayloadVariable::Tables(names)) => {
            let mut table = get_table(["tables"]);
            for name in names {
                table.add_row([name]);
            }

            println!("{}\n", table);
        }
        Payload::Select { labels, rows } => {
            let mut table = get_table(labels);
            for values in rows {
                let values: Vec<String> = values.into_iter().map(Into::into).collect();

                table.add_row(values);
            }

            println!("{}\n", table);
        }
        _ => {}
    };

    fn print_affected(n: usize, msg: &str) {
        println!("{} row{} {}\n", n, if n > 1 { "s" } else { "" }, msg);
    }
}

pub fn print_help() {
    const HEADER: [&str; 2] = ["command", "description"];
    const CONTENT: [[&str; 2]; 4] = [
        [".help", "show help"],
        [".quit", "quit program"],
        [".tables", "show table names"],
        [".version", "show version"],
    ];

    let mut table = get_table(HEADER);
    for row in CONTENT {
        table.add_row(row);
    }

    println!("{}\n", table);
}

fn get_table<T: Into<Row>>(header: T) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_BORDERS_ONLY)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(header);

    table
}
