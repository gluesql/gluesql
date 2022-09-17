use {
    crate::{
        command::{Command, CommandError},
        helper::CliHelper,
        print::Print,
    },
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
        rl.set_helper(Some(CliHelper::default()));

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

            rl.add_history_entry(&line);

            let command = match Command::parse(&line) {
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
                Err(CommandError::LackOfValue) => {
                    println!("[error] should specify value.\n");
                    continue;
                }
                Err(CommandError::WrongOption(e)) => {
                    println!("[error] cannot support option: {e}\n");
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
                Command::Execute(sql) => match self.glue.execute(sql.as_str()) {
                    Ok(payloads) => self.print.payloads(&payloads)?,
                    Err(e) => {
                        println!("[error] {}\n", e);
                    }
                },
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
                Command::Set(name, value) => {
                    if let Err(e) = self.print.set_option(name, value) {
                        println!("[error] {}\n", e);
                    }
                }
                Command::Show(name) => self.print.show_option(name)?,
            }
        }

        Ok(())
    }

    pub fn load<P: AsRef<Path>>(&mut self, filename: P) -> Result<()> {
        let mut sqls = String::new();
        File::open(filename)?.read_to_string(&mut sqls)?;
        for sql in sqls.split(';').filter(|sql| !sql.trim().is_empty()) {
            match self.glue.execute(sql) {
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
