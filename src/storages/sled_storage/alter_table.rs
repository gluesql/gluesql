#![cfg(feature = "alter-table")]

use {
    super::{error::err_into, fetch_schema, SledStorage},
    crate::{
        ast::ColumnDef, executor::evaluate_stateless, schema::ColumnDefExt, utils::Vector,
        AlterTable, AlterTableError, MutResult, Row, Schema, Value,
    },
    async_trait::async_trait,
    boolinator::Boolinator,
    std::{iter::once, str},
};

macro_rules! try_self {
    ($self: expr, $expr: expr) => {
        match $expr {
            Err(e) => {
                return Err(($self, e.into()));
            }
            Ok(v) => v,
        }
    };
}

macro_rules! try_into {
    ($self: expr, $expr: expr) => {
        try_self!($self, $expr.map_err(err_into))
    };
}

macro_rules! fetch_schema {
    ($self: expr, $tree: expr, $table_name: expr) => {{
        let (key, schema) = try_self!($self, fetch_schema($tree, $table_name));
        let schema = try_into!(
            $self,
            schema.ok_or_else(|| AlterTableError::TableNotFound($table_name.to_string()))
        );

        (key, schema)
    }};
}

#[async_trait(?Send)]
impl AlterTable for SledStorage {
    async fn rename_schema(self, table_name: &str, new_table_name: &str) -> MutResult<Self, ()> {
        let (
            _,
            Schema {
                column_defs,
                indexes,
                ..
            },
        ) = fetch_schema!(self, &self.tree, table_name);
        let schema = Schema {
            table_name: new_table_name.to_string(),
            column_defs,
            indexes,
        };

        let tree = &self.tree;

        // remove existing schema
        let key = format!("schema/{}", table_name);
        try_into!(self, tree.remove(key));

        // insert new schema
        let value = try_into!(self, bincode::serialize(&schema));
        let key = format!("schema/{}", new_table_name);
        let key = key.as_bytes();
        try_into!(self, self.tree.insert(key, value));

        // replace data
        let prefix = format!("data/{}/", table_name);

        for item in tree.scan_prefix(prefix.as_bytes()) {
            let (key, value) = try_into!(self, item);

            let new_key = try_into!(self, str::from_utf8(key.as_ref()));
            let new_key = new_key.replace(table_name, new_table_name);
            try_into!(self, tree.insert(new_key, value));

            try_into!(self, tree.remove(key));
        }

        Ok((self, ()))
    }

    async fn rename_column(
        self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> MutResult<Self, ()> {
        let (
            key,
            Schema {
                column_defs,
                indexes,
                ..
            },
        ) = fetch_schema!(self, &self.tree, table_name);

        let i = column_defs
            .iter()
            .position(|column_def| column_def.name == old_column_name)
            .ok_or(AlterTableError::RenamingColumnNotFound);
        let i = try_into!(self, i);

        let ColumnDef {
            data_type, options, ..
        } = column_defs[i].clone();

        let column_def = ColumnDef {
            name: new_column_name.to_owned(),
            data_type,
            options,
        };
        let column_defs = Vector::from(column_defs).update(i, column_def).into();

        let schema = Schema {
            table_name: table_name.to_string(),
            column_defs,
            indexes,
        };
        let value = try_into!(self, bincode::serialize(&schema));
        try_into!(self, self.tree.insert(key, value));

        Ok((self, ()))
    }

    async fn add_column(self, table_name: &str, column_def: &ColumnDef) -> MutResult<Self, ()> {
        let (
            key,
            Schema {
                table_name,
                column_defs,
                indexes,
            },
        ) = fetch_schema!(self, &self.tree, table_name);

        if column_defs
            .iter()
            .any(|ColumnDef { name, .. }| name == &column_def.name)
        {
            let adding_column = column_def.name.to_owned();

            return Err((
                self,
                AlterTableError::AddingColumnAlreadyExists(adding_column).into(),
            ));
        }

        let ColumnDef { data_type, .. } = column_def;
        let nullable = column_def.is_nullable();
        let default = column_def.get_default();
        let value = match (default, nullable) {
            (Some(expr), _) => {
                let evaluated = try_self!(self, evaluate_stateless(None, expr));

                try_self!(self, evaluated.try_into_value(&data_type, nullable))
            }
            (None, true) => Value::Null,
            (None, false) => {
                return Err((
                    self,
                    AlterTableError::DefaultValueRequired(format!("{:?}", column_def)).into(),
                ));
            }
        };

        // migrate data
        let prefix = format!("data/{}/", table_name);

        for item in self.tree.scan_prefix(prefix.as_bytes()) {
            let (key, row) = try_into!(self, item);
            let row: Row = try_into!(self, bincode::deserialize(&row));
            let row = Row(row.0.into_iter().chain(once(value.clone())).collect());
            let row = try_into!(self, bincode::serialize(&row));

            try_into!(self, self.tree.insert(key, row));
        }

        // update schema
        let column_defs = column_defs
            .into_iter()
            .chain(once(column_def.clone()))
            .collect::<Vec<ColumnDef>>();

        let schema = Schema {
            table_name,
            column_defs,
            indexes,
        };
        let schema_value = try_into!(self, bincode::serialize(&schema));
        try_into!(self, self.tree.insert(key, schema_value));

        Ok((self, ()))
    }

    async fn drop_column(
        self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> MutResult<Self, ()> {
        let (
            key,
            Schema {
                table_name,
                column_defs,
                indexes,
            },
        ) = fetch_schema!(self, &self.tree, table_name);

        let index = column_defs
            .iter()
            .position(|ColumnDef { name, .. }| name == column_name);

        let index = match (index, if_exists) {
            (Some(index), _) => index,
            (None, true) => {
                return Ok((self, ()));
            }
            (None, false) => {
                return Err((
                    self,
                    AlterTableError::DroppingColumnNotFound(column_name.to_string()).into(),
                ));
            }
        };

        // migrate data
        let prefix = format!("data/{}/", table_name);

        for item in self.tree.scan_prefix(prefix.as_bytes()) {
            let (key, row) = try_into!(self, item);
            let row: Row = try_into!(self, bincode::deserialize(&row));
            let row = Row(row
                .0
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| (i != index).as_some(v))
                .collect());
            let row = try_into!(self, bincode::serialize(&row));

            try_into!(self, self.tree.insert(key, row));
        }

        // update schema
        let column_defs = column_defs
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| (i != index).as_some(v))
            .collect::<Vec<ColumnDef>>();

        let schema = Schema {
            table_name,
            column_defs,
            indexes,
        };
        let schema_value = try_into!(self, bincode::serialize(&schema));
        try_into!(self, self.tree.insert(key, schema_value));

        Ok((self, ()))
    }
}
