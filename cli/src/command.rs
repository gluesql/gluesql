#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Quit,
    Execute(String),
}

impl Command {
    pub fn parse(line: &str) -> Result<Self, ()> {
        match line.trim_start().trim_end_matches(|c| c == ' ' || c == ';') {
            ".help" => Ok(Self::Help),
            ".quit" => Ok(Self::Quit),
            ".tables" => Ok(Self::Execute("SHOW TABLES".to_owned())),
            ".version" => Ok(Self::Execute("SHOW VERSION".to_owned())),
            _ if line.starts_with('.') => Err(()),
            sql => Ok(Self::Execute(sql.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_command() {
        use super::Command;

        assert_eq!(Ok(Command::Help), Command::parse(".help"));
        assert_eq!(Ok(Command::Help), Command::parse("   .help;"));
        assert_eq!(Ok(Command::Quit), Command::parse(".quit"));
        assert_eq!(Ok(Command::Quit), Command::parse(".quit;"));
        assert_eq!(Ok(Command::Quit), Command::parse(" .quit; "));
        assert_eq!(
            Ok(Command::Execute("SHOW TABLES".to_owned())),
            Command::parse(".tables")
        );
        assert_eq!(
            Ok(Command::Execute("SHOW VERSION".to_owned())),
            Command::parse(".version")
        );
        assert_eq!(Err(()), Command::parse(".foo"));
        assert_eq!(
            Ok(Command::Execute("SELECT * FROM Foo".to_owned())),
            Command::parse("SELECT * FROM Foo;")
        );
    }
}
