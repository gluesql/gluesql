use {
    crate::{
        command::{Command, CommandError},
        helper::CliHelper,
        print::Print,
    },
    edit::{edit_file, edit_with_builder, Builder},
    futures::executor::block_on,
    gluesql_core::{
        prelude::Glue,
        store::{GStore, GStoreMut},
    },
    rustyline::{error::ReadlineError, Editor},
    std::{
        error::Error,
        fs::File,
        io::{Read, Result, Write},
        path::Path,
    },
};

pub struct Cli<T, W>
where
    T: GStore + GStoreMut,
    W: Write,
{
    glue: Glue<T>,
    print: Print<W>,
}

impl<T, W> Cli<T, W>
where
    T: GStore + GStoreMut,
    W: Write,
{
    pub fn new(storage: T, output: W) -> Self {
        let glue = Glue::new(storage);
        let print = Print::new(output, None, Default::default());

        Self { glue, print }
    }

    pub fn run(&mut self) -> std::result::Result<(), Box<dyn Error>> {
        macro_rules! println {
            ($($p:tt),*) => ( writeln!(&mut self.print.output, $($p),*)?; )
        }

        self.print.help()?;

        let mut rl = Editor::<CliHelper>::new();
        rl.set_helper(Some(CliHelper));

        loop {
            let line = match rl.readline("gluesql> ") {
                Ok(line) => line,
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("bye\n");
                    break;
                }
                Err(e) => {
                    println!("[unknown error] {:?}", e);
                    break;
                }
            };

            let line = line.trim();
            if !(line.starts_with(".edit") || line.starts_with(".run")) {
                rl.add_history_entry(line);
            }

            let command = match Command::parse(line, &self.print.option) {
                Ok(command) => command,
                Err(CommandError::LackOfTable) => {
                    println!("[error] should specify table. eg: .columns TableName\n");
                    continue;
                }
                Err(CommandError::LackOfFile) => {
                    println!("[error] should specify file path.\n");
                    continue;
                }
                Err(CommandError::NotSupported) => {
                    println!("[error] command not supported: {}", line);
                    println!("\n  type .help to list all available commands.\n");
                    continue;
                }
                Err(CommandError::LackOfOption) => {
                    println!("[error] should specify option.\n");
                    continue;
                }
                Err(CommandError::LackOfValue(usage)) => {
                    println!("[error] should specify value.\n{usage}\n");
                    continue;
                }
                Err(CommandError::WrongOption(e)) => {
                    println!("[error] cannot support option: {e}\n");
                    continue;
                }
                Err(CommandError::LackOfSQLHistory) => {
                    println!("[error] Nothing in SQL history to run.\n");
                    continue;
                }
            };

            match command {
                Command::Help => {
                    self.print.help()?;
                    continue;
                }
                Command::Quit => {
                    println!("bye\n");
                    break;
                }
                Command::Execute(sql) => self.execute(sql)?,
                Command::ExecuteFromFile(filename) => {
                    if let Err(e) = self.load(&filename) {
                        println!("[error] {}\n", e);
                    }
                }
                Command::SpoolOn(path) => {
                    self.print.spool_on(path)?;
                }
                Command::SpoolOff => {
                    self.print.spool_off();
                }
                Command::Set(option) => self.print.set_option(option),
                Command::Show(option) => self.print.show_option(option)?,
                Command::Edit(file_name) => {
                    match file_name {
                        Some(file_name) => {
                            let file = Path::new(&file_name);
                            edit_file(file)?;
                        }
                        None => {
                            let mut builder = Builder::new();
                            builder.prefix("Glue_").suffix(".sql");
                            let last = rl.history().last().map_or_else(|| "", String::as_str);
                            let edited = edit_with_builder(last, &builder)?;
                            rl.add_history_entry(edited);
                        }
                    };
                }
                Command::Run => {
                    let sql = rl.history().last().ok_or(CommandError::LackOfSQLHistory);

                    match sql {
                        Ok(sql) => {
                            self.execute(sql)?;
                        }
                        Err(e) => {
                            println!("[error] {}\n", e);
                        }
                    };
                }
            }
        }

        Ok(())
    }

    fn execute(&mut self, sql: impl AsRef<str>) -> Result<()> {
        match block_on(self.glue.execute(sql)) {
            Ok(payloads) => self.print.payloads(&payloads)?,
            Err(e) => {
                println!("[error] {}\n", e);
            }
        };

        Ok(())
    }

    pub fn load<P: AsRef<Path>>(&mut self, filename: P) -> Result<()> {
        let mut sqls = String::new();
        File::open(filename)?.read_to_string(&mut sqls)?;
        for sql in sqls.split(';').filter(|sql| !sql.trim().is_empty()) {
            match block_on(self.glue.execute(sql)) {
                Ok(payloads) => self.print.payloads(&payloads)?,
                Err(e) => {
                    println!("[error] {}\n", e);
                    break;
                }
            }
        }

        Ok(())
    }
}
