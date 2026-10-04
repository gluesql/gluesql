use {
    proc_macro2::TokenStream,
    quote::quote,
    std::collections::BTreeSet,
    syn::{
        Expr, ExprLit, ItemFn, Lit, LitStr, Meta, Token, ext::IdentExt, parse::Parser,
        punctuated::Punctuated,
    },
};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let function: ItemFn = syn::parse2(item)?;
    if function.sig.constness.is_some() {
        return Err(syn::Error::new_spanned(
            &function.sig,
            "observe does not support const functions",
        ));
    }
    let mut name = LitStr::new(
        &function.sig.ident.unraw().to_string(),
        function.sig.ident.span(),
    );
    let mut target = LitStr::new("gluesql", function.sig.ident.span());
    let mut level = LitStr::new("debug", function.sig.ident.span());
    let mut fields = None;
    let mut seen = BTreeSet::new();
    for option in Punctuated::<Meta, Token![,]>::parse_terminated.parse2(attr)? {
        let key = option.path().get_ident().ok_or_else(|| {
            syn::Error::new_spanned(option.path(), "expected an observation option name")
        })?;
        if !seen.insert(key.to_string()) {
            return Err(syn::Error::new_spanned(
                option,
                "duplicate observation option",
            ));
        }
        match option {
            Meta::List(list) if list.path.is_ident("fields") => fields = Some(list.tokens),
            Meta::NameValue(value)
                if value.path.is_ident("name")
                    || value.path.is_ident("target")
                    || value.path.is_ident("level") =>
            {
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
                if value.path.is_ident("name") {
                    name = text;
                } else if value.path.is_ident("target") {
                    target = text;
                } else {
                    level = text;
                }
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "supported observation options are name, target, level, and fields",
                ));
            }
        }
    }
    if !matches!(
        level.value().as_str(),
        "trace" | "debug" | "info" | "warn" | "error"
    ) {
        return Err(syn::Error::new_spanned(level, "unsupported span level"));
    }
    let fields = fields.map(|fields| quote!(fields(#fields),));
    Ok(quote! {
        #[tracing::instrument(name = #name, target = #target, level = #level, skip_all, #fields)]
        #function
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, quote::quote};

    #[test]
    fn rejects_body_selectors_and_invalid_options() {
        let body = quote!(
            fn example() {}
        );
        for attr in [
            quote!(after_let(rows, record(n = 1))),
            quote!(before_let(rows)),
            quote!(after_loop(row, record(n = 1))),
            quote!(count_loop(binding = row, field = n)),
            quote!(on_ok(value, record(n = 1))),
            quote!(err(Debug)),
            quote!(event("entry")),
            quote!(start = before_let(rows)),
            quote!(end = after_let(rows)),
            quote!(record(n = 1)),
            quote!(level = "verbose"),
            quote!(name = "x", name = "y"),
            quote!(fields(n = 1), fields(n = 2)),
            quote!(target = 1),
        ] {
            assert!(
                expand(attr.clone(), body.clone()).is_err(),
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
