use {
    crate::{command::Command, helper::CliHelper, print::Print},
    gluesql::{
        prelude::Glue,
        store::{GStore, GStoreMut},
    },
    rustyline::{error::ReadlineError, Editor},
    std::{
        fmt::Debug,
        io::{Result, Write},
    },
};

pub struct Cli<T, U, W>
where
    T: Debug,
    U: GStore<T> + GStoreMut<T>,
    W: Write,
{
    glue: Glue<T, U>,
    print: Print<W>,
}

impl<T, U, W> Cli<T, U, W>
where
    T: Debug,
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
            }
        }

        Ok(())
    }
}
