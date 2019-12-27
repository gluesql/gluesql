use std::convert::From;

#[derive(Clone, Copy, Debug)]
pub enum QueryType {
    SELECT,
    INSERT,
    CREATE,
}

#[derive(Debug)]
pub enum ColumnType {
    BOOLEAN,
    INTEGER,
    SERIAL,
}

#[derive(Debug)]
pub enum Token {
    Query(QueryType),
    Column(ColumnType),
    Table,
    Etc(String),
}

impl From<String> for Token {
    fn from(s: String) -> Self {
        use QueryType as Q;
        use ColumnType as C;
        use Token::*;

        match s.to_uppercase().as_str() {
            "CREATE" => Query(Q::CREATE),
            "SELECT" => Query(Q::SELECT),
            "INSERT" => Query(Q::INSERT),
            "BOOLEAN" => Column(C::BOOLEAN),
            "INTEGER" => Column(C::INTEGER),
            "SERIAL" => Column(C::SERIAL),
            "TABLE" => Table,
            _ => Etc(s),
        }
    }
}
