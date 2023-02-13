use {
    crate::command::{SetOption, ShowOption},
    gluesql_core::prelude::{Payload, PayloadVariable},
    std::{
        collections::{HashMap, HashSet},
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
    pub fn tabular(&mut self, tabular: bool) {
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
            Payload::Create => self.write("Table created")?,
            Payload::DropTable => self.write("Table dropped")?,
            Payload::AlterTable => self.write("Table altered")?,
            Payload::CreateIndex => self.write("Index created")?,
            Payload::DropIndex => self.write("Index dropped")?,
            Payload::Commit => self.write("Commit completed")?,
            Payload::Rollback => self.write("Rollback completed")?,
            Payload::StartTransaction => self.write("Transaction started")?,
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
            Payload::Select { labels, rows } => match &self.option.tabular {
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
                    let PrintOption {
                        colsep,
                        colwrap,
                        heading,
                        ..
                    } = &self.option;

                    if *heading {
                        let labels = labels
                            .iter()
                            .map(|v| format!("{colwrap}{v}{colwrap}"))
                            .collect::<Vec<_>>()
                            .join(colsep.as_str());

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
            },
            Payload::SelectMap(rows) => {
                let mut labels = rows
                    .iter()
                    .flat_map(HashMap::keys)
                    .map(AsRef::as_ref)
                    .collect::<HashSet<&str>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                labels.sort();

                match &self.option.tabular {
                    true => {
                        let mut table = self.get_table(labels.clone());
                        for row in rows {
                            let row = labels
                                .iter()
                                .map(|label| {
                                    row.get(*label)
                                        .map(Into::into)
                                        .unwrap_or_else(|| "".to_owned())
                                })
                                .collect::<Vec<String>>();

                            table.add_record(row);
                        }
                        let table = self.build_table(table);
                        self.write(table)?;
                    }
                    false => {
                        let PrintOption {
                            colsep,
                            colwrap,
                            heading,
                            ..
                        } = &self.option;

                        if *heading {
                            let labels = labels
                                .iter()
                                .map(|v| format!("{colwrap}{v}{colwrap}"))
                                .collect::<Vec<_>>()
                                .join(colsep.as_str());

                            writeln!(self.output, "{}", labels)?;
                        }

                        for row in rows {
                            let row = labels
                                .iter()
                                .map(|label| {
                                    let v = row
                                        .get(*label)
                                        .map(Into::into)
                                        .unwrap_or_else(|| "".to_owned());

                                    format!("{colwrap}{v}{colwrap}")
                                })
                                .collect::<Vec<_>>()
                                .join(colsep.as_str());

                            writeln!(self.output, "{}", row)?
                        }
                    }
                }
            }
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
        const CONTENT: [[&str; 2]; 11] = [
            [".help", "show help"],
            [".quit", "quit program"],
            [".tables", "show table names"],
            [".columns TABLE", "show columns from TABLE"],
            [".version", "show version"],
            [".execute PATH", "execute SQL from PATH"],
            [".spool PATH|off", "spool to PATH or off"],
            [".show OPTION", "show print option eg).show all"],
            [".set OPTION", "set print option eg).set tabular off"],
            [".edit [PATH]", "open editor with last command or PATH"],
            [".run ", "execute last command"],
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
        std::path::PathBuf,
    };

    #[test]
    fn print_help() {
        let mut print = Print::new(Vec::new(), None, Default::default());

        let actual = {
            print.help().unwrap();

            String::from_utf8(print.output).unwrap()
        };
        let expected = "
| command         | description                           |
|-----------------|---------------------------------------|
| .help           | show help                             |
| .quit           | quit program                          |
| .tables         | show table names                      |
| .columns TABLE  | show columns from TABLE               |
| .version        | show version                          |
| .execute PATH   | execute SQL from PATH                 |
| .spool PATH|off | spool to PATH or off                  |
| .show OPTION    | show print option eg).show all        |
| .set OPTION     | set print option eg).set tabular off  |
| .edit [PATH]    | open editor with last command or PATH |
| .run            | execute last command                  |";

        assert_eq!(
            actual.as_str().trim_matches('\n'),
            expected.trim_matches('\n')
        );
    }

    #[test]
    fn print_payload() {
        use gluesql_core::{
            ast::DataType,
            prelude::{Payload, PayloadVariable, Value},
        };

        let mut print = Print::new(Vec::new(), None, Default::default());

        macro_rules! test {
            ($payload: expr, $expected: literal ) => {
                print.payloads(&[$payload]).unwrap();

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

        test!(Payload::Create, "Table created");
        test!(Payload::DropTable, "Table dropped");
        test!(Payload::AlterTable, "Table altered");
        test!(Payload::CreateIndex, "Index created");
        test!(Payload::DropIndex, "Index dropped");
        test!(Payload::Commit, "Commit completed");
        test!(Payload::Rollback, "Rollback completed");
        test!(Payload::StartTransaction, "Transaction started");
        test!(Payload::Insert(0), "0 row inserted");
        test!(Payload::Insert(1), "1 row inserted");
        test!(Payload::Insert(7), "7 rows inserted");
        test!(Payload::Delete(300), "300 rows deleted");
        test!(Payload::Update(123), "123 rows updated");
        test!(
            Payload::ShowVariable(PayloadVariable::Version("11.6.1989".to_owned())),
            "v11.6.1989"
        );
        test!(
            Payload::ShowVariable(PayloadVariable::Tables(Vec::new())),
            "
| tables |"
        );
        test!(
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
            Payload::Select {
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
            Payload::SelectMap(vec![
                [
                    ("id".to_owned(), Value::I64(1)),
                    ("title".to_owned(), Value::Str("foo".to_owned()))
                ]
                .into_iter()
                .collect(),
                [("id".to_owned(), Value::I64(2))].into_iter().collect(),
                [("title".to_owned(), Value::Str("bar".to_owned()))]
                    .into_iter()
                    .collect(),
            ]),
            "
| id | title |
|----|-------|
| 1  | foo   |
| 2  |       |
|    | bar   |"
        );

        test!(
            Payload::ShowColumns(vec![
                ("id".to_owned(), DataType::Int),
                ("name".to_owned(), DataType::Text),
                ("isabear".to_owned(), DataType::Boolean),
            ],),
            "
| Field   | Type    |
|---------|---------|
| id      | INT     |
| name    | TEXT    |
| isabear | BOOLEAN |"
        );

        test!(
            Payload::ShowColumns(vec![
                ("id".to_owned(), DataType::Int8),
                ("calc1".to_owned(), DataType::Float),
                ("cost".to_owned(), DataType::Decimal),
                ("DOB".to_owned(), DataType::Date),
                ("clock".to_owned(), DataType::Time),
                ("tstamp".to_owned(), DataType::Timestamp),
                ("ival".to_owned(), DataType::Interval),
                ("uuid".to_owned(), DataType::Uuid),
                ("hash".to_owned(), DataType::Map),
                ("mylist".to_owned(), DataType::List),
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
                ]
            },
            "
id|title|valid
1|foo|TRUE
2|bar|FALSE"
        );

        test!(
            Payload::SelectMap(vec![
                [
                    ("id".to_owned(), Value::I64(1)),
                    ("title".to_owned(), Value::Str("foo".to_owned()))
                ]
                .into_iter()
                .collect(),
                [("id".to_owned(), Value::I64(2))].into_iter().collect(),
                [("title".to_owned(), Value::Str("bar".to_owned()))]
                    .into_iter()
                    .collect(),
            ]),
            "
id|title
1|foo
2|
|bar"
        );

        // ".set colsep ," should set column separator as ","
        print.set_option(SetOption::Colsep(",".into()));
        assert_eq!(print.option.format(ShowOption::Colsep), r#"colsep ",""#);

        test!(
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
                ],
            },
            "
'1','foo','TRUE'
'2','bar','FALSE'"
        );

        // ".set header ON should print without column name"
        print.set_option(SetOption::Heading(true));
        test!(
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
                ],
            },
            "
'id','title','valid'
'1','foo','TRUE'
'2','bar','FALSE'"
        );

        // ".set tabular ON" should recover default option: colsep("|"), colwrap("")
        print.set_option(SetOption::Tabular(true));
        assert_eq!(print.option.format(ShowOption::Tabular), "tabular ON");
        assert_eq!(print.option.format(ShowOption::Colsep), r#"colsep "|""#);
        assert_eq!(print.option.format(ShowOption::Colwrap), r#"colwrap """#);
        assert_eq!(print.option.format(ShowOption::Heading), "heading ON");
        assert_eq!(
            print.option.format(ShowOption::All),
            "
tabular ON
colsep \"|\"
colwrap \"\"
heading ON"
                .trim_matches('\n')
        );
    }

    #[test]
    fn print_spool() {
        use std::fs;

        let mut print = Print::new(Vec::new(), None, Default::default());

        // Spooling on file
        fs::create_dir_all("tmp").unwrap();
        assert!(print.spool_on(PathBuf::from("tmp/spool.txt")).is_ok());
        assert!(print.write("Test").is_ok());
        assert!(print.show_option(ShowOption::All).is_ok());
        print.spool_off();
        assert!(print.write("Test").is_ok());
    }
}
