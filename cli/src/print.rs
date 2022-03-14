use {
    comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_BORDERS_ONLY, Row, Table},
    gluesql_core::prelude::{Payload, PayloadVariable},
    std::io::{Result, Write},
};

pub struct Print<W: Write> {
    pub output: W,
}

impl<W: Write> Print<W> {
    pub fn new(output: W) -> Self {
        Print { output }
    }

    pub fn payload(&mut self, payload: Payload) -> Result<()> {
        let mut affected = |n: usize, msg: &str| -> Result<()> {
            writeln!(
                self.output,
                "{} row{} {}\n",
                n,
                if n > 1 { "s" } else { "" },
                msg
            )
        };

        match payload {
            Payload::Insert(n) => affected(n, "inserted")?,
            Payload::Delete(n) => affected(n, "deleted")?,
            Payload::Update(n) => affected(n, "updated")?,
            Payload::ShowVariable(PayloadVariable::Version(v)) => {
                writeln!(self.output, "v{}\n", v)?
            }
            Payload::ShowVariable(PayloadVariable::Tables(names)) => {
                let mut table = get_table(["tables"]);
                for name in names {
                    table.add_row([name]);
                }

                writeln!(self.output, "{}\n", table)?;
            }
            Payload::Select { labels, rows } => {
                let mut table = get_table(labels);
                for values in rows {
                    let values: Vec<String> = values.into_iter().map(Into::into).collect();

                    table.add_row(values);
                }

                writeln!(self.output, "{}\n", table)?;
            }
            _ => {}
        };

        Ok(())
    }

    pub fn help(&mut self) -> Result<()> {
        const HEADER: [&str; 2] = ["command", "description"];
        const CONTENT: [[&str; 2]; 5] = [
            [".help", "show help"],
            [".quit", "quit program"],
            [".tables", "show table names"],
            [".version", "show version"],
            [".execute FILE", "execute SQL from a file"],
        ];

        let mut table = get_table(HEADER);
        for row in CONTENT {
            table.add_row(row);
        }

        writeln!(self.output, "{}\n", table)
    }
}

fn get_table<T: Into<Row>>(header: T) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_BORDERS_ONLY)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(header);

    table
}

#[cfg(test)]
mod tests {
    use super::Print;

    #[test]
    fn print_help() {
        let mut print = Print::new(Vec::new());

        let expected = "
╭─────────────────────────────────────────╮
│ command         description             │
╞═════════════════════════════════════════╡
│ .help           show help               │
│ .quit           quit program            │
│ .tables         show table names        │
│ .version        show version            │
│ .execute FILE   execute SQL from a file │
╰─────────────────────────────────────────╯";
        let found = {
            print.help().unwrap();

            String::from_utf8(print.output).unwrap()
        };

        assert_eq!(
            expected.trim_matches('\n'),
            found.as_str().trim_matches('\n')
        );
    }

    #[test]
    fn print_payload() {
        use gluesql_core::prelude::{Payload, PayloadVariable, Value};

        let mut print = Print::new(Vec::new());

        macro_rules! test {
            ($expected: literal, $payload: expr) => {
                print.payload($payload).unwrap();

                assert_eq!(
                    $expected.trim_matches('\n'),
                    String::from_utf8(print.output.clone())
                        .unwrap()
                        .as_str()
                        .trim_matches('\n')
                );

                print.output.clear();
            };
        }

        test!("0 row inserted", Payload::Insert(0));
        test!("1 row inserted", Payload::Insert(1));
        test!("7 rows inserted", Payload::Insert(7));
        test!("300 rows deleted", Payload::Delete(300));
        test!("123 rows updated", Payload::Update(123));
        test!(
            "v11.6.1989",
            Payload::ShowVariable(PayloadVariable::Version("11.6.1989".to_owned()))
        );
        test!(
            "
╭────────╮
│ tables │
╞════════╡
╰────────╯",
            Payload::ShowVariable(PayloadVariable::Tables(Vec::new()))
        );
        test!(
            "
╭──────────────────╮
│ tables           │
╞══════════════════╡
│ Allocator        │
│ ExtendFromWithin │
│ IntoRawParts     │
│ Reserve          │
│ Splice           │
╰──────────────────╯",
            Payload::ShowVariable(PayloadVariable::Tables(
                [
                    "Allocator",
                    "ExtendFromWithin",
                    "IntoRawParts",
                    "Reserve",
                    "Splice",
                ]
                .into_iter()
                .map(ToOwned::to_owned)
                .collect()
            ))
        );
        test!(
            "
╭──────╮
│ id   │
╞══════╡
│ 101  │
│ 202  │
│ 301  │
│ 505  │
│ 1001 │
╰──────╯",
            Payload::Select {
                labels: vec!["id".to_owned()],
                rows: [101, 202, 301, 505, 1001]
                    .into_iter()
                    .map(Value::I64)
                    .map(|v| vec![v])
                    .collect::<Vec<Vec<Value>>>(),
            }
        );
        test!(
            "
╭────────────────────╮
│ id   title   valid │
╞════════════════════╡
│ 1    foo     TRUE  │
│ 2    bar     FALSE │
│ 3    bas     FALSE │
│ 4    lim     TRUE  │
│ 5    kim     TRUE  │
╰────────────────────╯",
            Payload::Select {
                labels: ["id", "title", "valid"]
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect(),
                rows: vec![
                    vec![
                        Value::I64(1),
                        Value::Str("foo".to_owned()),
                        Value::Bool(true)
                    ],
                    vec![
                        Value::I64(2),
                        Value::Str("bar".to_owned()),
                        Value::Bool(false)
                    ],
                    vec![
                        Value::I64(3),
                        Value::Str("bas".to_owned()),
                        Value::Bool(false)
                    ],
                    vec![
                        Value::I64(4),
                        Value::Str("lim".to_owned()),
                        Value::Bool(true)
                    ],
                    vec![
                        Value::I64(5),
                        Value::Str("kim".to_owned()),
                        Value::Bool(true)
                    ],
                ],
            }
        );
    }
}
