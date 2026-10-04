use {
    crate::observe,
    proc_macro2::TokenStream,
    quote::quote,
    std::collections::BTreeSet,
    syn::{
        Ident, ImplItem, ItemImpl, MetaList, Token, Type, ext::IdentExt, parse::Parser,
        punctuated::Punctuated,
    },
};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let mut implementation: ItemImpl = syn::parse2(item)?;
    let skip = if attr.is_empty() {
        Vec::new()
    } else {
        let list = (|input: syn::parse::ParseStream| {
            let list = input.parse::<MetaList>()?;
            input.parse::<Option<Token![,]>>()?;
            Ok(list)
        })
        .parse2(attr)?;
        if !list.path.is_ident("skip") {
            return Err(syn::Error::new_spanned(
                list,
                "the only supported storage option is skip",
            ));
        }
        list.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?
            .into_iter()
            .collect()
    };
    let name = match implementation.self_ty.as_ref() {
        Type::Path(ty) => ty
            .path
            .segments
            .last()
            .map(|segment| segment.ident.unraw().to_string()),
        _ => None,
    }
    .ok_or_else(|| {
        syn::Error::new_spanned(
            &implementation.self_ty,
            "trace_storage requires a named implementation type",
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
        *method = syn::parse2(observe::instrument(
            &syn::parse2(quote!(#method))?,
            &span_name,
            "trace",
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
        assert!(expand(quote!(skip(stream),), implementation.clone()).is_ok());
        for options in [
            quote!(iterators(stream)),
            quote!(capture = "full"),
            quote!(skip(missing)),
            quote!(skip(stream, stream)),
            quote!(skip(stream), skip(stream)),
            quote!(name = "custom"),
        ] {
            assert!(expand(options, implementation.clone()).is_err());
        }
    }

    #[test]
    fn infers_self_type_names() {
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
        }
        let raw = expand(quote!(), quote!(impl r#type { fn r#match(&self) {} }))
            .unwrap()
            .to_string();
        assert!(raw.contains("\"gluesql.type.match\""));
        let tuple = quote!(impl Store for (Storage, Storage) { fn operation(&self) {} });
        assert!(expand(quote!(), tuple.clone()).is_err());
        assert!(expand(quote!(name = "pair"), tuple).is_err());
    }
}
