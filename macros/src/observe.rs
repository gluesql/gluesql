use {
    proc_macro2::TokenStream,
    quote::quote,
    syn::{ItemFn, ext::IdentExt},
};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "observe does not accept options",
        ));
    }
    let function: ItemFn = syn::parse2(item)?;
    let name = function.sig.ident.unraw().to_string();
    instrument(&function, &name, "debug")
}

pub(super) fn instrument(function: &ItemFn, name: &str, level: &str) -> syn::Result<TokenStream> {
    if function.sig.constness.is_some() {
        return Err(syn::Error::new_spanned(
            &function.sig,
            "observe does not support const functions",
        ));
    }
    Ok(quote! {
        #[tracing::instrument(name = #name, target = "gluesql", level = #level, skip_all)]
        #function
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, quote::quote};

    #[test]
    fn rejects_options_and_const_functions() {
        for attr in [
            quote!(name = "custom"),
            quote!(target = "custom"),
            quote!(level = "info"),
            quote!(fields(n = 1)),
            quote!(after_let(rows, record(n = 1))),
            quote!(count_loop(binding = row, field = n)),
            quote!(on_ok(value, record(n = 1))),
            quote!(err(Debug)),
        ] {
            assert!(
                expand(
                    attr.clone(),
                    quote!(
                        fn example() {}
                    )
                )
                .is_err(),
                "accepted {attr}"
            );
        }
        assert!(
            expand(
                quote!(),
                quote!(
                    const fn example() {}
                )
            )
            .is_err()
        );
    }
}
