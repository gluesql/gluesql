use gluesql_pm::basic_derives;

#[basic_derives]
pub struct Query {
    pub body: String,
    pub limit: Option<String>,
    pub offset: Option<String>,
}

fn main() {}
