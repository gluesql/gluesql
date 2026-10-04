use {
    crate::observe,
    proc_macro2::TokenStream,
    quote::quote,
    std::collections::BTreeSet,
    syn::{
        Expr, ExprLit, Ident, ImplItem, ItemImpl, Lit, Meta, Token, Type, ext::IdentExt,
        parse::Parser, punctuated::Punctuated,
    },
};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let mut implementation: ItemImpl = syn::parse2(item)?;
    let mut name = None;
    let mut skip = Vec::<Ident>::new();
    let mut seen = BTreeSet::new();
    for option in Punctuated::<Meta, Token![,]>::parse_terminated.parse2(attr)? {
        let key = option.path().get_ident().ok_or_else(|| {
            syn::Error::new_spanned(option.path(), "expected a storage option name")
        })?;
        if !seen.insert(key.to_string()) {
            return Err(syn::Error::new_spanned(option, "duplicate storage option"));
        }
        match option {
            Meta::List(list) if list.path.is_ident("skip") => {
                skip = list
                    .parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?
                    .into_iter()
                    .collect();
            }
            Meta::NameValue(value) if value.path.is_ident("name") => {
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(text),
                    ..
                }) = value.value
                else {
                    return Err(syn::Error::new_spanned(
                        value.value,
                        "expected a string literal",
                    ));
                };
                name = Some(text.value());
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "supported storage options are name and skip",
                ));
            }
        }
    }
    let name = name
        .or_else(|| match implementation.self_ty.as_ref() {
            Type::Path(ty) => ty
                .path
                .segments
                .last()
                .map(|segment| segment.ident.unraw().to_string()),
            _ => None,
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(
                &implementation.self_ty,
                "specify name for a non-path implementation type",
            )
        })?;
    let mut selected = BTreeSet::new();
    for ident in &skip {
        if !selected.insert(ident.to_string()) {
            return Err(syn::Error::new_spanned(ident, "duplicate skipped method"));
        }
        if !implementation
            .items
            .iter()
            .any(|item| matches!(item, ImplItem::Fn(method) if method.sig.ident == *ident))
        {
            return Err(syn::Error::new_spanned(
                ident,
                "skipped method was not found in this impl",
            ));
        }
    }
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        if skip.contains(&method.sig.ident) {
            continue;
        }
        let span_name = format!("gluesql.{name}.{}", method.sig.ident.unraw());
        *method = syn::parse2(observe::expand(
            quote!(name = #span_name, level = "trace"),
            quote!(#method),
        )?)?;
    }
    Ok(quote!(#implementation))
}

#[cfg(test)]
mod tests {
    use {super::expand, quote::quote};

    #[test]
    fn rejects_removed_options_and_invalid_skips() {
        let implementation = quote!(impl Storage {
            fn stream(&self) -> Result<Rows> { todo!() }
        });
        for options in [
            quote!(name = "test", iterators(stream)),
            quote!(name = "test", capture = "full"),
            quote!(name = "test", skip(missing)),
            quote!(name = "test", skip(stream, stream)),
            quote!(name = "test", skip(stream), skip(stream)),
            quote!(name = "test", name = "other"),
        ] {
            assert!(expand(options, implementation.clone()).is_err());
        }
    }

    #[test]
    fn infers_self_type_names_and_preserves_overrides() {
        for implementation in [
            quote!(impl Storage { fn operation(&self) {} }),
            quote!(impl Store for Storage { fn operation(&self) {} }),
            quote!(
                impl<T> Store for backend::Storage<T> {
                    fn operation(&self) {}
                }
            ),
        ] {
            let inferred = expand(quote!(), implementation.clone())
                .unwrap()
                .to_string();
            assert!(inferred.contains("\"gluesql.Storage.operation\""));
            let explicit = expand(quote!(name = "custom"), implementation)
                .unwrap()
                .to_string();
            assert!(explicit.contains("\"gluesql.custom.operation\""));
        }
        let raw = expand(quote!(), quote!(impl r#type { fn r#match(&self) {} }))
            .unwrap()
            .to_string();
        assert!(raw.contains("\"gluesql.type.match\""));
        let tuple = quote!(impl Store for (Storage, Storage) { fn operation(&self) {} });
        assert!(expand(quote!(), tuple.clone()).is_err());
        assert!(expand(quote!(name = "pair"), tuple).is_ok());
    }
}
