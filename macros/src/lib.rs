use {
    proc_macro::TokenStream,
    proc_macro_crate::{FoundCrate, crate_name},
    syn::{
        Attribute, DeriveInput, Expr, ExprLit, Lit, MetaNameValue, parse_macro_input,
        spanned::Spanned,
    },
};

mod from_glue_row;
mod to_glue_row;

fn resolve_gluesql_crate() -> Result<syn::Path, syn::Error> {
    if std::env::var("CARGO_PKG_NAME")
        .map(|name| name == "gluesql")
        .unwrap_or(false)
    {
        return Ok(syn::parse_quote!(::gluesql::core));
    }

    if let Ok(found) = crate_name("gluesql") {
        let path = match found {
            FoundCrate::Itself => syn::parse_quote!(crate::core),
            FoundCrate::Name(name) => {
                let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
                syn::parse_quote!(::#ident::core)
            }
        };

        return Ok(path);
    }

    let found = crate_name("gluesql_core")
        .or_else(|_| crate_name("gluesql-core"))
        .map_err(|_| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "failed to locate `gluesql` crate; add a dependency on `gluesql` or `gluesql-core`",
            )
        })?;

    let path = match found {
        FoundCrate::Itself => syn::parse_quote!(crate),
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            syn::parse_quote!(::#ident)
        }
    };

    Ok(path)
}

#[proc_macro_derive(FromGlueRow, attributes(glue))]
pub fn derive_from_glue_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match from_glue_row::expand_from_glue_row(input) {
        Ok(ts) => TokenStream::from(ts),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(ToGlueRow, attributes(glue))]
pub fn derive_to_glue_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match to_glue_row::expand_to_glue_row(input) {
        Ok(ts) => TokenStream::from(ts),
        Err(e) => e.to_compile_error().into(),
    }
}

fn parse_glue_rename(attr: &Attribute) -> Option<Result<Option<String>, syn::Error>> {
    if !attr.path().is_ident("glue") {
        return None;
    }
    match attr.parse_args::<MetaNameValue>() {
        Ok(MetaNameValue { path, value, .. }) if path.is_ident("rename") => match value {
            Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) => Some(Ok(Some(s.value()))),
            other => Some(Err(syn::Error::new(
                other.span(),
                "expected string literal for rename",
            ))),
        },
        Ok(_) => Some(Ok(None)),
        Err(e) => Some(Err(e)),
    }
}
