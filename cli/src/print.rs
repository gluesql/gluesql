use {
    crate::command::{SetOption, ShowOption},
    gluesql_core::{
        ast::ToSql,
        prelude::{Payload, PayloadVariable},
    },
    std::{
        fmt::Display,
        fs::File,
        io::{Result as IOResult, Write},
        path::Path,
    },
    tabled::{builder::Builder, Style, Table},
};

pub struct Print<W: Write> {
    pub output: W,
    spool_file: Option<File>,
    pub option: PrintOption,
}

pub struct PrintOption {
    pub tabular: bool,
    colsep: String,
    colwrap: String,
    heading: bool,
}

impl PrintOption {
    fn tabular(&mut self, tabular: bool) {
        match tabular {
            true => {
                self.tabular = tabular;
                self.colsep("|".into());
                self.colwrap("".into());
                self.heading(true);
            }
            false => self.tabular = tabular,
        }
    }

    fn colsep(&mut self, colsep: String) {
        self.colsep = colsep;
    }

    fn colwrap(&mut self, colwrap: String) {
        self.colwrap = colwrap;
    }

    fn heading(&mut self, heading: bool) {
        self.heading = heading;
    }

    fn format(&self, option: ShowOption) -> String {
        fn string_from(value: &bool) -> String {
            match value {
                true => "ON".into(),
                false => "OFF".into(),
            }
        }
        match option {
            ShowOption::Tabular => format!("tabular {}", string_from(&self.tabular)),
            ShowOption::Colsep => format!("colsep \"{}\"", self.colsep),
            ShowOption::Colwrap => format!("colwrap \"{}\"", self.colwrap),
            ShowOption::Heading => format!("heading {}", string_from(&self.heading)),
            ShowOption::All => format!(
                "{}\n{}\n{}\n{}",
                self.format(ShowOption::Tabular),
                self.format(ShowOption::Colsep),
                self.format(ShowOption::Colwrap),
                self.format(ShowOption::Heading),
            ),
        }
    }
}

impl Default for PrintOption {
    fn default() -> Self {
        Self {
            tabular: true,
            colsep: "|".into(),
            colwrap: "".into(),
            heading: true,
        }
    }
}

impl<'a, W: Write> Print<W> {
    pub fn new(output: W, spool_file: Option<File>, option: PrintOption) -> Self {
        Print {
            output,
            spool_file,
            option,
        }
    }

    pub fn payloads(&mut self, payloads: &[Payload]) -> IOResult<()> {
        payloads.iter().try_for_each(|p| self.payload(p))
    }

    pub fn payload(&mut self, payload: &Payload) -> IOResult<()> {
        let mut affected = |n: usize, msg: &str| -> IOResult<()> {
            let payload = format!("{} row{} {}", n, if n > 1 { "s" } else { "" }, msg);
            self.write(payload)
        };

        match payload {
            Payload::Insert(n) => affected(*n, "inserted")?,
            Payload::Delete(n) => affected(*n, "deleted")?,
            Payload::Update(n) => affected(*n, "updated")?,
            Payload::ShowVariable(PayloadVariable::Version(v)) => self.write(format!("v{v}"))?,
            Payload::ShowVariable(PayloadVariable::Tables(names)) => {
                let mut table = self.get_table(["tables"]);
                for name in names {
                    table.add_record([name]);
                }
                let table = self.build_table(table);
                self.write(table)?;
            }
            Payload::ShowColumns(columns) => {
                let mut table = self.get_table(vec!["Field", "Type"]);
                for (field, field_type) in columns {
                    table.add_record([field, &field_type.to_string()]);
                }
                let table = self.build_table(table);
                self.write(table)?;
            }
            Payload::ShowIndexes(indexes) => {
                let mut table = self.get_table(vec!["Index Name", "Order", "Description"]);
                for index in indexes {
                    table.add_record([
                        index.name.to_string(),
                        index.order.to_string(),
                        index.expr.to_sql(),
                    ]);
                }
                let table = self.build_table(table);
                self.write(table)?;
            }
            Payload::Select { labels, rows } => {
                let PrintOption {
                    tabular,
                    colsep,
                    colwrap,
                    heading,
                } = &self.option;

                match tabular {
                    true => {
                        let labels = labels.iter().map(AsRef::as_ref);
                        let mut table = self.get_table(labels);
                        for row in rows {
                            let row: Vec<String> = row.iter().map(Into::into).collect();

                            table.add_record(row);
                        }
                        let table = self.build_table(table);
                        self.write(table)?;
                    }
                    false => {
                        let labels = labels
                            .iter()
                            .map(|v| format!("{colwrap}{v}{colwrap}"))
                            .collect::<Vec<_>>()
                            .join(colsep.as_str());
                        if *heading {
                            writeln!(self.output, "{}", labels)?;
                        }

                        for row in rows {
                            let row = row
                                .iter()
                                .map(Into::into)
                                .map(|v: String| format!("{colwrap}{v}{colwrap}"))
                                .collect::<Vec<_>>()
                                .join(colsep.as_str());
                            writeln!(self.output, "{}", row)?
                        }
                    }
                }
            }
            _ => {}
        };

        Ok(())
    }

    fn write(&mut self, payload: impl Display) -> IOResult<()> {
        if let Some(file) = &self.spool_file {
            writeln!(file.to_owned(), "{}\n", payload)?;
        };

        writeln!(self.output, "{}\n", payload)
    }

    pub fn help(&mut self) -> IOResult<()> {
        const HEADER: [&str; 2] = ["command", "description"];
        const CONTENT: [[&str; 2]; 9] = [
            [".help", "show help"],
            [".quit", "quit program"],
            [".tables", "show table names"],
            [".columns TABLE", "show columns from TABLE"],
            [".version", "show version"],
            [".execute FILE", "execute SQL from a file"],
            [".spool FILE|off", "spool to file or off"],
            [".show OPTION", "show print option eg).show all"],
            [".set OPTION", "set print option eg).set tabular off"],
        ];

        let mut table = self.get_table(HEADER);
        for row in CONTENT {
            table.add_record(row);
        }
        let table = self.build_table(table);

        writeln!(self.output, "{}\n", table)
    }

    pub fn spool_on<P: AsRef<Path>>(&mut self, filename: P) -> IOResult<()> {
        let file = File::create(filename)?;
        self.spool_file = Some(file);

        Ok(())
    }

    pub fn spool_off(&mut self) {
        self.spool_file = None;
    }

    fn get_table<T: IntoIterator<Item = &'a str>>(&self, headers: T) -> Builder {
        let mut table = Builder::default();
        table.set_columns(headers);

        table
    }

    fn build_table(&self, builder: Builder) -> Table {
        builder.build().with(Style::markdown())
    }

    pub fn set_option(&mut self, option: SetOption) {
        match option {
            SetOption::Tabular(value) => self.option.tabular(value),
            SetOption::Colsep(value) => self.option.colsep(value),
            SetOption::Colwrap(value) => self.option.colwrap(value),
            SetOption::Heading(value) => self.option.heading(value),
        }
    }

    pub fn show_option(&mut self, option: ShowOption) -> IOResult<()> {
        let payload = self.option.format(option);
        self.write(payload)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::Print,
        crate::command::{SetOption, ShowOption},
        gluesql_core::{data::SchemaIndex, data::SchemaIndexOrd},
    };

    #[test]
    fn print_help() {
        let mut print = Print::new(Vec::new(), None, Default::default());

        let actual = {
            print.help().unwrap();

            String::from_utf8(print.output).unwrap()
        };
        let expected = "
| command         | description                          |
|-----------------|--------------------------------------|
| .help           | show help                            |
| .quit           | quit program                         |
| .tables         | show table names                     |
| .columns TABLE  | show columns from TABLE              |
| .version        | show version                         |
| .execute FILE   | execute SQL from a file              |
| .spool FILE|off | spool to file or off                 |
| .show OPTION    | show print option eg).show all       |
| .set OPTION     | set print option eg).set tabular off |";

        assert_eq!(
            actual.as_str().trim_matches('\n'),
            expected.trim_matches('\n')
        );
    }

    #[test]
    fn print_payload() {
        use gluesql_core::{
            ast::{BinaryOperator, DataType, Expr},
            prelude::{Payload, PayloadVariable, Value},
        };

        let mut print = Print::new(Vec::new(), None, Default::default());

        macro_rules! test {
            ($payload: expr, $expected: literal ) => {
                print.payload($payload).unwrap();

                assert_eq!(
                    String::from_utf8(print.output.clone())
                        .unwrap()
                        .as_str()
                        .trim_matches('\n'),
                    $expected.trim_matches('\n')
                );

                print.output.clear();
            };
        }

        test!(&Payload::Insert(0), "0 row inserted");
        test!(&Payload::Insert(1), "1 row inserted");
        test!(&Payload::Insert(7), "7 rows inserted");
        test!(&Payload::Delete(300), "300 rows deleted");
        test!(&Payload::Update(123), "123 rows updated");
        test!(
            &Payload::ShowVariable(PayloadVariable::Version("11.6.1989".to_owned())),
            "v11.6.1989"
        );
        test!(
            &Payload::ShowVariable(PayloadVariable::Tables(Vec::new())),
            "
| tables |"
        );
        test!(
            &Payload::ShowVariable(PayloadVariable::Tables(
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
            )),
            "
| tables           |
|------------------|
| Allocator        |
| ExtendFromWithin |
| IntoRawParts     |
| Reserve          |
| Splice           |"
        );
        test!(
            &Payload::Select {
                labels: vec!["id".to_owned()],
                rows: [101, 202, 301, 505, 1001]
                    .into_iter()
                    .map(Value::I64)
                    .map(|v| vec![v])
                    .collect::<Vec<Vec<Value>>>(),
            },
            "
| id   |
|------|
| 101  |
| 202  |
| 301  |
| 505  |
| 1001 |"
        );
        test!(
            &Payload::Select {
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
            },
            "
| id | title | valid |
|----|-------|-------|
| 1  | foo   | TRUE  |
| 2  | bar   | FALSE |
| 3  | bas   | FALSE |
| 4  | lim   | TRUE  |
| 5  | kim   | TRUE  |"
        );

        test!(
            &Payload::ShowIndexes(vec![
                SchemaIndex {
                    name: "id_ndx".to_string(),
                    order: SchemaIndexOrd::Asc,
                    expr: Expr::Identifier("id".to_string())
                },
                SchemaIndex {
                    name: "name_ndx".to_string(),
                    order: SchemaIndexOrd::Desc,
                    expr: Expr::Identifier("name".to_string())
                },
                SchemaIndex {
                    name: "expr_ndx".to_string(),
                    order: SchemaIndexOrd::Both,
                    expr: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier("expr1".to_string())),
                        op: BinaryOperator::Minus,
                        right: Box::new(Expr::Identifier("expr2".to_string()))
                    }
                }
            ],),
            "
| Index Name | Order | Description   |
|------------|-------|---------------|
| id_ndx     | ASC   | id            |
| name_ndx   | DESC  | name          |
| expr_ndx   | BOTH  | expr1 - expr2 |"
        );

        test!(
            &Payload::ShowColumns(vec![
                ("id".to_string(), DataType::Int),
                ("name".to_string(), DataType::Text),
                ("isabear".to_string(), DataType::Boolean),
            ],),
            "
| Field   | Type    |
|---------|---------|
| id      | INT     |
| name    | TEXT    |
| isabear | BOOLEAN |"
        );

        test!(
            &Payload::ShowColumns(vec![
                ("id".to_string(), DataType::Int8),
                ("calc1".to_string(), DataType::Float),
                ("cost".to_string(), DataType::Decimal),
                ("DOB".to_string(), DataType::Date),
                ("clock".to_string(), DataType::Time),
                ("tstamp".to_string(), DataType::Timestamp),
                ("ival".to_string(), DataType::Interval),
                ("uuid".to_string(), DataType::Uuid),
                ("hash".to_string(), DataType::Map),
                ("mylist".to_string(), DataType::List),
            ],),
            "
| Field  | Type      |
|--------|-----------|
| id     | INT8      |
| calc1  | FLOAT     |
| cost   | DECIMAL   |
| DOB    | DATE      |
| clock  | TIME      |
| tstamp | TIMESTAMP |
| ival   | INTERVAL  |
| uuid   | UUID      |
| hash   | MAP       |
| mylist | LIST      |"
        );

        // ".set tabular OFF" should print SELECTED payload without tabular option
        print.set_option(SetOption::Tabular(false));
        test!(
            &Payload::Select {
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
                ],
            },
            "
id|title|valid
1|foo|TRUE
2|bar|FALSE"
        );

        // ".set colsep ," should set column separator as ","
        print.set_option(SetOption::Colsep(",".into()));
        assert_eq!(print.option.format(ShowOption::Colsep), r#"colsep ",""#);

        test!(
            &Payload::Select {
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
                ],
            },
            "
id,title,valid
1,foo,TRUE
2,bar,FALSE"
        );

        // ".set colwrap '" should set column separator as "'"
        print.set_option(SetOption::Colwrap("'".into()));
        assert_eq!(print.option.format(ShowOption::Colwrap), r#"colwrap "'""#);
        test!(
            &Payload::Select {
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
                ],
            },
            "
'id','title','valid'
'1','foo','TRUE'
'2','bar','FALSE'"
        );

        // ".set header OFF should print without column name"
        print.set_option(SetOption::Heading(false));
        test!(
            &Payload::Select {
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
                ],
            },
            "
'1','foo','TRUE'
'2','bar','FALSE'"
        );

        // ".set tabular ON" should recover default option: colsep("|"), colwrap("")
        print.set_option(SetOption::Tabular(true));
        assert_eq!(print.option.format(ShowOption::Tabular), "tabular ON");
        assert_eq!(print.option.format(ShowOption::Colsep), r#"colsep "|""#);
        assert_eq!(print.option.format(ShowOption::Colwrap), r#"colwrap """#);
        assert_eq!(print.option.format(ShowOption::Heading), "heading ON");
    }
}
