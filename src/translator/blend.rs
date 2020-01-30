use nom_sql::{Column, FieldDefinitionExpression, Literal};
use std::convert::From;

pub struct Blend<'a> {
    fields: &'a Vec<FieldDefinitionExpression>,
}

impl Blend<'_> {
    pub fn check(&self, item: &(Column, Literal)) -> bool {
        self.fields.iter().any(|expr| match expr {
            FieldDefinitionExpression::All => true,
            FieldDefinitionExpression::AllInTable(_) => unimplemented!(),
            FieldDefinitionExpression::Col(column) => column.name == item.0.name,
            FieldDefinitionExpression::Value(_) => unimplemented!(),
        })
    }
}

impl<'a> From<&'a Vec<FieldDefinitionExpression>> for Blend<'a> {
    fn from(fields: &'a Vec<FieldDefinitionExpression>) -> Self {
        Blend { fields }
    }
}
