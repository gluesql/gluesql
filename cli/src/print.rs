use std::error::Error;

use gluesql_core::prelude::Value;
use tabled::{
    object::{Rows, Segment},
    Disable, Format, Header, Modify, Padding, Panel, TableIteratorExt,
};

use {
    crate::command::CommandError,
    gluesql_core::{
        ast::ToSql,
        prelude::{Payload, PayloadVariable},
    },
    std::{
        fmt::Display,
        fs::File,
        io::{Result as IOResult, Write},
        path::Path,
        result::Result,
    },
    tabled::{builder::Builder, Style, Table},
};

pub struct Print<W: Write> {
    pub output: W,
    spool_file: Option<File>,
    option: PrintOption,
}

#[derive(Clone)]
pub struct PrintOption {
    tabular: Tabular,
    heading: bool,
}

#[derive(Clone)]
enum Tabular {
    On,
    Off { colsep: String, colwrap: String },
}

impl Tabular {
    fn get_option(&self) -> (&str, &str) {
        match self {
            Tabular::On => ("|", ""),
            Tabular::Off { colsep, colwrap } => (colsep, colwrap),
        }
    }

    fn get_string(&self) -> &str {
        match self {
            Tabular::On => "ON",
            Tabular::Off { .. } => "OFF",
        }
    }

    fn update_option(self, name: &str, value: &str) -> Result<Tabular, CommandError> {
        let get_char = |v: &str| match v.len() {
            1 => v.chars().next().ok_or_else(|| -> CommandError {
                CommandError::WrongOption("colsep length should be 1".into())
            }),
            _ => Err(CommandError::WrongOption("colsep length should 1".into())),
        };

        let result = match self {
            Tabular::On => return Err(CommandError::WrongOption("run .set tabular OFF".into())),
            Tabular::Off { colsep, colwrap } => match name {
                "colsep" => Tabular::Off {
                    colsep: value.to_string(),
                    colwrap,
                },
                "colwrap" => Tabular::Off {
                    colsep,
                    colwrap: value.into(),
                },
                _ => todo!(),
            },
        };

        Ok(result)
    }
}

impl<'a> PrintOption {
    fn to_show(&self, name: String) -> Result<String, CommandError> {
        let payload = match name.to_lowercase().as_str() {
            "colsep" => format!("colsep \"{}\"", self.tabular.get_option().0),
            "colwrap" => format!("colwrap \"{}\"", self.tabular.get_option().1),
            "tabular" => format!("tabular {}", &self.tabular.get_string()),
            "heading" => format!("heading {}", string_from(&self.heading)),
            "all" => format!(
                "{}\n{}\n{}\n{}",
                self.to_show("colsep".into())?,
                self.to_show("colwrap".into())?,
                self.to_show("tabular".into())?,
                self.to_show("heading".into())?
            ),
            option => return Err(CommandError::WrongOption(option.into()).into()),
        };

        Ok(payload)
    }

    fn set_tabular(&mut self, value: String) -> Result<(), CommandError> {
        match value.to_uppercase().as_ref() {
            "ON" => self.tabular = Tabular::On,
            "OFF" => {
                self.tabular = Tabular::Off {
                    colsep: "|".into(),
                    colwrap: "".into(),
                }
            }
            _ => todo!(),
        }

        Ok(())
    }
}

fn string_from(value: &bool) -> &str {
    match value {
        true => "ON",
        false => "OFF",
    }
}

impl<'a> Default for PrintOption {
    fn default() -> Self {
        Self {
            tabular: Tabular::On,
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
                // // let table = names.table().with(Disable::Row(..1)).with(Header("tables"));
                // let headers = ["tables"];
                // let table = self.get_table(headers);

                // self.write(table)?;
                let mut table = self.get_table(["tables"]);
                for name in names {
                    table.add_record([name]);
                }
                let table = self.build_table(table);
                self.write(table)?;
            }
            Payload::ShowColumns(columns) => {
                // let data = columns
                //     .iter()
                //     .map(|(field, field_type)| [field, &field_type.to_string()])
                //     .collect::<Vec<_>>();

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
            Payload::Select { labels, rows } => match &self.option.tabular {
                Tabular::On => {
                    let labels = labels.iter().map(AsRef::as_ref);
                    let mut table = self.get_table(labels);
                    for row in rows {
                        let row: Vec<String> = row.iter().map(Into::into).collect();

                        table.add_record(row);
                    }
                    let table = self.build_table(table);
                    self.write(table)?;
                }
                Tabular::Off { colsep, colwrap } => {
                    let labels = labels
                        .iter()
                        .map(|v| format!("{colwrap}{v}{colwrap}"))
                        .collect::<Vec<_>>()
                        .join(&colsep.to_string());
                    writeln!(self.output, "{}", labels);

                    rows.iter().for_each(|row| {
                        let row = row
                            .iter()
                            .map(Into::into)
                            .map(|v: String| format!("{colwrap}{v}{colwrap}"))
                            .collect::<Vec<_>>()
                            .join(&colsep.to_string());
                        writeln!(self.output, "{}", row);
                    });
                }
            },
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
        const CONTENT: [[&str; 2]; 7] = [
            [".help", "show help"],
            [".quit", "quit program"],
            [".tables", "show table names"],
            [".columns TABLE", "show columns from TABLE"],
            [".version", "show version"],
            [".execute FILE", "execute SQL from a file"],
            [".spool FILE|off", "spool to file or off"],
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

    pub fn set_option(&mut self, name: String, value: String) -> Result<(), CommandError> {
        let bool_from = |value: String| -> Result<bool, CommandError> {
            match value.to_uppercase().as_str() {
                "ON" => Ok(true),
                "OFF" => Ok(false),
                _ => Err(CommandError::WrongOption(value)),
            }
        };

        match name.to_lowercase().as_str() {
            "tabular" => self.option.set_tabular(value),
            "colsep" => {
                self.option.tabular = self
                    .option
                    .tabular
                    .clone()
                    .update_option(name.as_ref(), value.as_ref())?;

                Ok(())
            }

            "colwrap" => {
                self.option.tabular = self
                    .option
                    .tabular
                    .clone()
                    .update_option(name.as_ref(), value.as_ref())?;

                Ok(())
            }
            "heading" => {
                self.option.heading = bool_from(value)?;

                Ok(())
            }
            _ => return Err(CommandError::WrongOption(name)),
        }?;

        Ok(())
    }

    pub fn show_option(&mut self, name: String) -> Result<(), Box<dyn Error>> {
        let payload = self.option.to_show(name);
        self.write(payload?)?;

        Ok(())
    }

    fn get_table<T: IntoIterator<Item = &'a str>>(&self, headers: T) -> Builder {
        let mut table = Builder::default();

        match self.option.heading {
            true => table.set_columns(headers).clone(),
            false => table,
        }
    }

    fn build_table(&self, builder: Builder) -> Table {
        let builder = builder.build().with(Style::markdown());

        builder
        // match self.option.tabular.clone() {
        //     Tabular::On => builder,
        //     Tabular::Off { colsep, colwrap } => {
        //         let colsep = Style::empty().vertical(colsep);
        //         let padding_zero = Modify::new(Segment::all()).with(Padding::new(0, 0, 0, 0));
        //         let wrapped_data = Modify::new(Segment::all())
        //             .with(Format::new(|data| format!("{colwrap}{data}{colwrap}")));

        //         builder.with(padding_zero).with(colsep).with(wrapped_data)
        //     }
        // }
    }

    // fn get_table2<T: IntoIterator<Item = &'a str>>(&self, headers: T, names: &[String]) -> Table {
    //     let table = names.table().with(Disable::Row(..1)).with(Header(names));

    //     table
    // }
}

#[cfg(test)]
mod tests {
    use super::Print;
    use gluesql_core::{data::SchemaIndex, data::SchemaIndexOrd};

    #[test]
    fn print_help() {
        let mut print = Print::new(Vec::new(), None, Default::default());

        let expected = "
| command         | description             |
|-----------------|-------------------------|
| .help           | show help               |
| .quit           | quit program            |
| .tables         | show table names        |
| .columns TABLE  | show columns from TABLE |
| .version        | show version            |
| .execute FILE   | execute SQL from a file |
| .spool FILE|off | spool to file or off    |";
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
        use gluesql_core::{
            ast::{BinaryOperator, DataType, Expr},
            prelude::{Payload, PayloadVariable, Value},
        };

        let mut print = Print::new(Vec::new(), None, Default::default());

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

        test!("0 row inserted", &Payload::Insert(0));
        test!("1 row inserted", &Payload::Insert(1));
        test!("7 rows inserted", &Payload::Insert(7));
        test!("300 rows deleted", &Payload::Delete(300));
        test!("123 rows updated", &Payload::Update(123));
        test!(
            "v11.6.1989",
            &Payload::ShowVariable(PayloadVariable::Version("11.6.1989".to_owned()))
        );
        test!(
            "
| tables |",
            &Payload::ShowVariable(PayloadVariable::Tables(Vec::new()))
        );
        test!(
            "
| tables           |
|------------------|
| Allocator        |
| ExtendFromWithin |
| IntoRawParts     |
| Reserve          |
| Splice           |",
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
            ))
        );
        test!(
            "
| id   |
|------|
| 101  |
| 202  |
| 301  |
| 505  |
| 1001 |",
            &Payload::Select {
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
| id | title | valid |
|----|-------|-------|
| 1  | foo   | TRUE  |
| 2  | bar   | FALSE |
| 3  | bas   | FALSE |
| 4  | lim   | TRUE  |
| 5  | kim   | TRUE  |",
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
            }
        );

        test!(
            "
| Index Name | Order | Description   |
|------------|-------|---------------|
| id_ndx     | ASC   | id            |
| name_ndx   | DESC  | name          |
| expr_ndx   | BOTH  | expr1 - expr2 |",
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
            ],)
        );

        test!(
            "
| Field   | Type    |
|---------|---------|
| id      | INT     |
| name    | TEXT    |
| isabear | BOOLEAN |",
            &Payload::ShowColumns(vec![
                ("id".to_string(), DataType::Int),
                ("name".to_string(), DataType::Text),
                ("isabear".to_string(), DataType::Boolean),
            ],)
        );

        test!(
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
| mylist | LIST      |",
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
            ],)
        );

        print.set_option("tabular".into(), "off".into()).unwrap();
        test!(
            "
id|title|valid
1 |foo  |TRUE 
2 |bar  |FALSE",
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
            }
        );

        print.set_option("colsep".into(), ",".into()).unwrap();
        test!(
            "
id,title,valid
1,foo,TRUE 
2,bar,FALSE",
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
            }
        );

        //         print.set_option("colwrap".into(), "'".into()).unwrap();
        //         test!(
        //             "
        // 'id','title','valid'
        // '1','foo','TRUE'
        // '2','bar','FALSE'",
        //             &Payload::Select {
        //                 labels: ["id", "title", "valid"]
        //                     .into_iter()
        //                     .map(ToOwned::to_owned)
        //                     .collect(),
        //                 rows: vec![
        //                     vec![
        //                         Value::I64(1),
        //                         Value::Str("foo".to_owned()),
        //                         Value::Bool(true)
        //                     ],
        //                     vec![
        //                         Value::I64(2),
        //                         Value::Str("bar".to_owned()),
        //                         Value::Bool(false)
        //                     ],
        //                 ],
        //             }
        //         );
    }
}
