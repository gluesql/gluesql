use {
    crate::{observe, resolve_gluesql_crate},
    proc_macro2::TokenStream,
    quote::quote,
    std::collections::BTreeSet,
    syn::{
        Expr, ExprLit, FnArg, GenericArgument, Ident, ImplItem, ItemImpl, Lit, Meta, Pat,
        PathArguments, ReturnType, Token, Type, parse::Parser, punctuated::Punctuated,
    },
};

struct Args {
    name: String,
    capture_full: bool,
    iterators: Vec<Ident>,
    skip: Vec<Ident>,
}

impl Args {
    fn parse(tokens: TokenStream) -> Result<Self, syn::Error> {
        let values = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(tokens)?;
        let mut name = None;
        let mut capture_full = true;
        let mut iterators = Vec::new();
        let mut skip = Vec::new();
        let mut seen = BTreeSet::new();

        for option in values {
            let key = option.path().get_ident().ok_or_else(|| {
                syn::Error::new_spanned(option.path(), "expected a storage option name")
            })?;
            if !seen.insert(key.to_string()) {
                return Err(syn::Error::new_spanned(option, "duplicate storage option"));
            }
            if let Meta::List(list) = &option
                && (list.path.is_ident("iterators") || list.path.is_ident("skip"))
            {
                let methods = list
                    .parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?
                    .into_iter()
                    .collect();
                if list.path.is_ident("iterators") {
                    iterators = methods;
                } else {
                    skip = methods;
                }
                continue;
            }
            let Meta::NameValue(syn::MetaNameValue { path, value, .. }) = option else {
                return Err(syn::Error::new_spanned(
                    option,
                    "unsupported storage option",
                ));
            };
            let Expr::Lit(ExprLit {
                lit: Lit::Str(value),
                ..
            }) = value
            else {
                return Err(syn::Error::new_spanned(value, "expected a string literal"));
            };

            if path.is_ident("name") {
                name = Some(value.value());
            } else if path.is_ident("capture") {
                capture_full = parse_mode(&value, "capture")?;
            } else {
                return Err(syn::Error::new_spanned(path, "unsupported option"));
            }
        }

        Ok(Self {
            name: name.ok_or_else(|| {
                syn::Error::new(proc_macro2::Span::call_site(), "missing `name = \"...\"`")
            })?,
            capture_full,
            iterators,
            skip,
        })
    }
}

fn parse_mode(value: &syn::LitStr, option: &str) -> Result<bool, syn::Error> {
    match value.value().as_str() {
        "full" => Ok(true),
        "off" => Ok(false),
        _ => Err(syn::Error::new(
            value.span(),
            format!("`{option}` must be `full` or `off`"),
        )),
    }
}

pub fn expand(attr: TokenStream, item: TokenStream) -> Result<TokenStream, syn::Error> {
    let args = Args::parse(attr)?;
    let mut implementation: ItemImpl = syn::parse2(item)?;
    let gluesql = resolve_gluesql_crate()?;
    let mut selected = BTreeSet::new();
    for name in args.iterators.iter().chain(&args.skip) {
        if !selected.insert(name.to_string()) {
            return Err(syn::Error::new_spanned(
                name,
                "duplicate or conflicting method selection",
            ));
        }
        if !implementation
            .items
            .iter()
            .any(|item| matches!(item, ImplItem::Fn(method) if method.sig.ident == *name))
        {
            return Err(syn::Error::new_spanned(
                name,
                "selected method was not found in this impl",
            ));
        }
    }
    let trait_name = implementation
        .trait_
        .as_ref()
        .and_then(|(_, path, _)| path.segments.last())
        .map(|segment| segment.ident.to_string());

    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };

        if args.skip.contains(&method.sig.ident) {
            if method
                .attrs
                .iter()
                .any(|attribute| attribute.path().is_ident("trace_iterator"))
            {
                return Err(syn::Error::new_spanned(
                    &method.sig.ident,
                    "skipped methods cannot use trace_iterator",
                ));
            }
            continue;
        }

        let method_name = method.sig.ident.to_string();
        let span_name = format!("gluesql.{}.{method_name}", args.name);
        let mut fields = Vec::new();

        for input in &method.sig.inputs {
            let FnArg::Typed(input) = input else {
                continue;
            };
            let Pat::Ident(pattern) = input.pat.as_ref() else {
                continue;
            };
            let ident = &pattern.ident;
            if args.capture_full {
                fields.push(quote!(#ident = ?#ident));
            }
            if matches!(
                (
                    trait_name.as_deref(),
                    method_name.as_str(),
                    ident.to_string().as_str()
                ),
                (Some("StoreMut"), "append_data" | "insert_data", "rows")
                    | (Some("StoreMut"), "delete_data", "keys")
            ) {
                fields.push(quote!(row_count = #ident.len()));
            }
        }

        let fields = (!fields.is_empty()).then(|| quote!(fields(#(#fields),*),));
        let error = (args.capture_full && result_ok_type(&method.sig.output).is_some())
            .then(|| quote!(err(Debug),));
        let explicitly_traced = method
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("trace_iterator"));
        method
            .attrs
            .retain(|attribute| !attribute.path().is_ident("trace_iterator"));
        let should_trace_iterator = explicitly_traced
            || args.iterators.contains(&method.sig.ident)
            || matches!(
                (trait_name.as_deref(), method_name.as_str()),
                (Some("Store"), "scan_data")
                    | (Some("Index"), "scan_indexed_data")
                    | (Some("Metadata"), "scan_table_meta")
            );

        if should_trace_iterator {
            let capture_full = args.capture_full;
            let ok_type = result_ok_type(&method.sig.output).ok_or_else(|| {
                syn::Error::new_spanned(
                    &method.sig.output,
                    "traced iterator methods must return `Result<Box<dyn Iterator<...>>>`",
                )
            })?;
            let iterator_operation = match method_name.as_str() {
                "scan_data" => "scan_rows".to_owned(),
                "scan_indexed_data" => "scan_indexed_rows".to_owned(),
                _ => format!("{method_name}_rows"),
            };
            let iterator_span_name = format!("gluesql.{}.{iterator_operation}", args.name);
            let block = &method.block;
            method.block = syn::parse_quote!({
                let __gluesql_result = (|| #block)();
                __gluesql_result.map(|__gluesql_iterator| {
                    let __gluesql_span = tracing::trace_span!(
                        target: "gluesql",
                        #iterator_span_name,
                        row_count = tracing::field::Empty,
                        error_count = tracing::field::Empty,
                        completed = tracing::field::Empty
                    );
                    let __gluesql_iterator: #ok_type = Box::new(
                        #gluesql::__private::TracedResultIterator::new(
                            __gluesql_iterator,
                            __gluesql_span,
                            #capture_full,
                        ),
                    );
                    __gluesql_iterator
                })
            });
        }
        *method = syn::parse2(observe::expand(
            quote!(target = "gluesql", name = #span_name, level = "trace", #fields #error),
            quote!(#method),
        )?)?;
    }

    Ok(quote!(#implementation))
}

fn result_ok_type(output: &ReturnType) -> Option<Type> {
    let ReturnType::Type(_, output) = output else {
        return None;
    };
    let Type::Path(output) = output.as_ref() else {
        return None;
    };
    let result = output.path.segments.last()?;
    if result.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &result.arguments else {
        return None;
    };

    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ok_type) => Some(ok_type.clone()),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, quote::quote};

    #[test]
    fn rejects_invalid_iterator_options() {
        let implementation = quote!(impl Storage {
            fn stream(&self) -> Result<Rows> { todo!() }
        });
        for options in [
            quote!(name = "test", iterators(missing)),
            quote!(name = "test", iterators(stream, stream)),
            quote!(name = "test", iterators(stream), iterators(stream)),
            quote!(name = "test", name = "other"),
            quote!(name = "test", skip(missing)),
            quote!(name = "test", skip(stream, stream)),
            quote!(name = "test", skip(stream), iterators(stream)),
        ] {
            assert!(expand(options, implementation.clone()).is_err());
        }
    }

    #[test]
    fn automatically_wraps_only_known_trait_methods() {
        for (implementation, wrapped) in [
            (
                quote!(impl gluesql_core::store::Store for Storage {
                    fn scan_data(&self) -> Result<Rows> { todo!() }
                }),
                true,
            ),
            (
                quote!(impl Index for Storage {
                    fn scan_indexed_data(&self) -> Result<Rows> { todo!() }
                }),
                true,
            ),
            (
                quote!(impl ExternalStore for Storage {
                    fn scan_data(&self) -> Vec<u8> { todo!() }
                }),
                false,
            ),
        ] {
            let expanded = expand(quote!(name = "test"), implementation)
                .unwrap()
                .to_string();
            assert_eq!(expanded.contains("TracedResultIterator"), wrapped);
        }
    }

    #[test]
    fn capture_off_omits_error_recording() {
        let implementation = quote! {
            impl Storage {
                fn operation(&self) -> Result<(), Error> {
                    Ok(())
                }
            }
        };
        let capture_full = expand(
            quote!(name = "test", capture = "full"),
            implementation.clone(),
        )
        .expect("full instrumentation should expand")
        .to_string();
        let capture_off = expand(quote!(name = "test", capture = "off"), implementation)
            .expect("timing-only instrumentation should expand")
            .to_string();

        assert!(capture_full.contains("tracing :: error !"));
        assert!(!capture_off.contains("tracing :: error !"));
    }
}
