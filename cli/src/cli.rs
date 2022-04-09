use {
    crate::{command::Command, helper::CliHelper, print::Print},
    gluesql_core::{
        prelude::Glue,
        store::{GStore, GStoreMut},
    },
    rustyline::{error::ReadlineError, Editor},
    std::{
        fs::File,
        io::{Read, Result, Write},
        path::Path,
    },
};

pub struct Cli<T, U, W>
where
    U: GStore<T> + GStoreMut<T>,
    W: Write,
{
    glue: Glue<T, U>,
    print: Print<W>,
}

impl<T, U, W> Cli<T, U, W>
where
    U: GStore<T> + GStoreMut<T>,
    W: Write,
{
    pub fn new(storage: U, output: W) -> Self {
        let glue = Glue::new(storage);
        let print = Print::new(output);

        Self { glue, print }
    }

    pub fn run(&mut self) -> Result<()> {
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
                Err(_) => {
                    println!("[error] command not supported: {}", line);
                    println!("\n  type .help to list all available commands.\n");
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
                    Ok(payload) => self.print.payload(payload)?,
                    Err(e) => {
                        println!("[error] {}\n", e);
                    }
                },
                Command::ExecuteFromFile(filename) => {
                    if let Err(e) = self.load(&filename) {
                        println!("[error] {}\n", e);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn load<P: AsRef<Path>>(&mut self, filename: P) -> Result<()> {
        let mut sqls = String::new();
        File::open(filename)?.read_to_string(&mut sqls)?;
        for sql in sqls.split(';').filter(|sql| !sql.trim().is_empty()) {
            match self.glue.execute(sql) {
                Ok(payload) => self.print.payload(payload)?,
                Err(e) => {
                    println!("[error] {}\n", e);
                    break;
                }
            }
        }

        Ok(())
    }
}
