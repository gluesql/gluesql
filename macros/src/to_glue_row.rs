use {
    crate::{parse_glue_rename, resolve_gluesql_crate},
    quote::quote,
    syn::{Data, DeriveInput, Fields, spanned::Spanned},
};

pub(crate) fn expand_to_glue_row(
    input: DeriveInput,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let input_span = input.span();

    let gluesql_crate_path = resolve_gluesql_crate()?;
    let gluesql_crate = quote! { #gluesql_crate_path };

    let ident = input.ident.clone();
    let Data::Struct(data) = input.data else {
        return Err(syn::Error::new(
            input_span,
            "ToGlueRow can only be derived for structs",
        ));
    };

    let fields = match data.fields {
        Fields::Named(f) => f.named,
        _ => {
            return Err(syn::Error::new(
                input_span,
                "ToGlueRow supports only named fields",
            ));
        }
    };

    let mut seen_columns: Vec<String> = Vec::new();
    let mut column_names = Vec::new();
    let mut field_literals = Vec::new();

    for field in &fields {
        let field_ident = field.ident.clone().expect("named field");
        let field_name_literal = field_ident.to_string();

        let mut rename: Option<String> = None;
        for attr in &field.attrs {
            if let Some(res) = parse_glue_rename(attr) {
                match res {
                    Ok(Some(name)) => rename = Some(name),
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        let column_name = rename.unwrap_or_else(|| field_name_literal.clone());
        if seen_columns.contains(&column_name) {
            return Err(syn::Error::new(
                field.span(),
                format!("duplicate column name `{column_name}`"),
            ));
        }
        seen_columns.push(column_name.clone());
        column_names.push(quote! { #column_name });

        field_literals.push(quote! {
            #gluesql_crate::translate::IntoParamLiteral::into_param_literal(
                ::core::clone::Clone::clone(&self.#field_ident)
            )
        });
    }

    let columns_len = column_names.len();

    let expanded = quote! {
        impl #gluesql_crate::row_conversion::ToGlueRow for #ident {
            fn glue_columns() -> &'static [&'static str] {
                static COLUMNS: [&str; #columns_len] = [ #(#column_names),* ];
                &COLUMNS
            }

            fn to_glue_row(&self) -> ::std::vec::Vec<#gluesql_crate::translate::ParamLiteral> {
                ::std::vec![ #(#field_literals),* ]
            }
        }
    };

    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::expand_to_glue_row;
    use syn::parse_quote;

    #[test]
    fn non_struct_input_returns_error() {
        let di: syn::DeriveInput = parse_quote! {
            enum E { A }
        };
        let err = expand_to_glue_row(di).unwrap_err();
        assert!(
            err.to_string()
                .contains("ToGlueRow can only be derived for structs")
        );
    }

    #[test]
    fn non_named_fields_struct_returns_error() {
        let di: syn::DeriveInput = parse_quote! {
            struct T(i32);
        };
        let err = expand_to_glue_row(di).unwrap_err();
        assert!(
            err.to_string()
                .contains("ToGlueRow supports only named fields")
        );
    }

    #[test]
    fn glue_rename_ok_some_and_ok_none() {
        let di: syn::DeriveInput = parse_quote! {
            struct S {
                #[glue(rename = "col")] a: i64,
                #[glue(other = "x")] b: String,
            }
        };
        let _ = expand_to_glue_row(di).expect("expand ok");
    }

    #[test]
    fn duplicate_column_name_returns_error() {
        let di: syn::DeriveInput = parse_quote! {
            struct S {
                value: i64,
                #[glue(rename = "value")]
                previous_value: i64,
            }
        };
        let err = expand_to_glue_row(di).unwrap_err();
        assert!(err.to_string().contains("duplicate column name `value`"));
    }

    #[test]
    fn glue_rename_wrong_literal_type() {
        let di: syn::DeriveInput = parse_quote! {
            struct S { #[glue(rename = 123)] a: i64 }
        };
        let err = expand_to_glue_row(di).unwrap_err();
        assert!(
            err.to_string()
                .contains("expected string literal for rename")
        );
    }
}
