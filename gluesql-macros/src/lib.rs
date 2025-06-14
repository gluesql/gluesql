use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(FromRow)]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(ref data) => &data.fields,
        _ => panic!("FromRow can only be derived for structs"),
    };

    let mut field_assignments = Vec::new();

    match fields {
        Fields::Named(named) => {
            for field in &named.named {
                let ident = field.ident.as_ref().expect("expected named fields");
                let ty = &field.ty;
                let field_name_str = ident.to_string();
                field_assignments.push(quote! {
                    #ident: <#ty as std::convert::TryFrom<&gluesql_core::data::Value>>::try_from(
                        row.get_value(#field_name_str).expect(concat!("missing field '", #field_name_str, "'"))
                    ).expect(concat!("failed to convert field '", #field_name_str, "'"))
                });
            }
        }
        _ => panic!("FromRow only supports named fields"),
    }

    let expanded = quote! {
        impl gluesql_core::FromRow for #name {
            fn from_row(row: &gluesql_core::data::Row) -> gluesql_core::error::Result<Self> {
                Ok(Self {
                    #(#field_assignments,)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
