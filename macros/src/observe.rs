use {
    proc_macro2::TokenStream,
    quote::quote,
    syn::{Attribute, ItemFn, Signature, ext::IdentExt, parse_quote},
};

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "observe does not accept options",
        ));
    }
    let mut function: ItemFn = syn::parse2(item)?;
    let name = function.sig.ident.unraw().to_string();
    function
        .attrs
        .insert(0, instrument_attribute(&function.sig, &name, "debug")?);
    Ok(quote!(#function))
}

pub(super) fn instrument_attribute(
    signature: &Signature,
    name: &str,
    level: &str,
) -> syn::Result<Attribute> {
    if signature.constness.is_some() {
        return Err(syn::Error::new_spanned(
            signature,
            "observe does not support const functions",
        ));
    }
    Ok(parse_quote!(
        #[tracing::instrument(name = #name, target = "gluesql", level = #level, skip_all)]
    ))
}

#[cfg(test)]
mod tests {
    use {super::expand, quote::quote};

    #[test]
    fn rejects_options_and_const_functions() {
        assert!(
            expand(
                quote!(name = "custom"),
                quote!(
                    fn example() {}
                )
            )
            .is_err()
        );
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
