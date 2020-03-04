use nom_sql::{Column, FieldDefinitionExpression};

pub struct Blend<'a> {
    pub fields: &'a Vec<FieldDefinitionExpression>,
}

impl Blend<'_> {
    pub fn check(&self, columns: &Vec<Column>, index: usize) -> bool {
        self.fields.iter().any(|expr| match expr {
            FieldDefinitionExpression::All => true,
            FieldDefinitionExpression::AllInTable(_) => unimplemented!(),
            FieldDefinitionExpression::Col(column) => column.name == columns[index].name,
            FieldDefinitionExpression::Value(_) => unimplemented!(),
        })
    }
}
