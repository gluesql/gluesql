use nom_sql::{Column, FieldDefinitionExpression};
use std::convert::From;

pub struct Blend<'a> {
    fields: &'a Vec<FieldDefinitionExpression>,
}

impl<'a> From<&'a Vec<FieldDefinitionExpression>> for Blend<'a> {
    fn from(fields: &'a Vec<FieldDefinitionExpression>) -> Self {
        Blend { fields }
    }
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
