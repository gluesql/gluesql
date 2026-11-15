use {
    super::{super::SourceColumns, PreparedSource, SourceRows},
    crate::{data::Row, plan::NativeSourcePlan, store::NativeQuery},
    std::rc::Rc,
};

pub(super) fn execute<'a>(
    storage: &'a impl NativeQuery,
    native: &'a NativeSourcePlan,
) -> PreparedSource<'a> {
    let names = native
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let names: Rc<[String]> = Rc::from(names);

    let output_types = native
        .columns
        .iter()
        .map(|column| column.data_type.clone())
        .collect::<Vec<_>>();

    let output = SourceColumns {
        alias: &native.alias,
        names: Rc::clone(&names),
    };

    let rows = Box::new(move |_| {
        let native_rows = storage.query_native(&native.query, &native.params, &output_types)?;

        let columns = Rc::clone(&names);
        let rows = native_rows.map(move |values| {
            let values = values?;

            Ok(Row {
                columns: Rc::clone(&columns),
                values,
            })
        });

        Ok(SourceRows {
            source: SourceColumns {
                alias: &native.alias,
                names: Rc::clone(&names),
            },
            rows: Box::new(rows),
        })
    });

    PreparedSource { output, rows }
}
