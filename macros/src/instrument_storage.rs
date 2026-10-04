use {
    crate::observe,
    proc_macro2::TokenStream,
    quote::quote,
    syn::{ImplItem, ItemImpl, Type, ext::IdentExt},
};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "trace_storage does not accept options",
        ));
    }
    let mut implementation: ItemImpl = syn::parse2(item)?;
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
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let span_name = format!("gluesql.{name}.{}", method.sig.ident.unraw());
        method.attrs.insert(
            0,
            observe::instrument_attribute(&method.sig, &span_name, "trace")?,
        );
    }
    Ok(quote!(#implementation))
}

#[cfg(test)]
mod tests {
    use {super::expand, quote::quote};

    #[test]
    fn rejects_options_and_const_methods() {
        assert!(
            expand(
                quote!(skip(stream)),
                quote!(impl Storage { fn stream(&self) {} })
            )
            .is_err()
        );
        assert!(expand(quote!(), quote!(impl Storage { const fn identity() {} })).is_err());
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
            let inferred = expand(quote!(), implementation).unwrap().to_string();
            assert!(inferred.contains("\"gluesql.Storage.operation\""));
        }
        let raw = expand(quote!(), quote!(impl r#type { fn r#match(&self) {} }))
            .unwrap()
            .to_string();
        assert!(raw.contains("\"gluesql.type.match\""));
        let tuple = quote!(impl Store for (Storage, Storage) { fn operation(&self) {} });
        assert!(expand(quote!(), tuple).is_err());
    }
}
