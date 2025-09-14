use {
    proc_macro::TokenStream,
    quote::quote,
    syn::{
        Attribute, Data, DeriveInput, Expr, ExprLit, Fields, Lit, MetaNameValue, Type,
        parse_macro_input, spanned::Spanned,
    },
};

// Testable helper: build implementation or return a syn::Error for invalid inputs.
fn expand_from_glue_row(input: DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    let input_span = input.span();

    let ident = input.ident.clone();
    let data = match input.data {
        Data::Struct(s) => s,
        _ => {
            return Err(syn::Error::new(
                input_span,
                "FromGlueRow can only be derived for structs",
            ));
        }
    };

    let fields = match data.fields {
        Fields::Named(f) => f.named,
        _ => {
            return Err(syn::Error::new(
                input_span,
                "FromGlueRow supports only named fields",
            ));
        }
    };

    let mut field_inits = Vec::new();
    let mut field_inits_with_idx = Vec::new();
    let mut field_idents = Vec::new();
    let mut fields_meta_pairs = Vec::new();

    for field in fields.iter() {
        let field_ident = field.ident.clone().expect("named field");
        field_idents.push(field_ident.clone());
        let field_name_literal = field_ident.to_string();

        // parse #[glue(rename = "...")]
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
        fields_meta_pairs.push(quote! { (#field_name_literal, #column_name) });

        // detect Option<T> and inner type
        let (is_option, base_ty) = match get_option_inner_type(&field.ty) {
            Some(inner) => (true, inner.clone()),
            None => (false, field.ty.clone()),
        };

        // compute expected kind and matching arms
        let (_expected_str, value_match_ref, value_match_idx) =
            match_expected(&base_ty, &field_name_literal, &column_name);

        let init_expr = if is_option {
            quote! {
                let __idx = __labels.iter().position(|l| l == #column_name)
                    .ok_or(::gluesql::core::row_conversion::RowConversionError::MissingColumn { field: #field_name_literal, column: #column_name })?;
                let __v = &__row[__idx];
                let #field_ident = match __v {
                    ::gluesql::core::data::Value::Null => None,
                    __v => Some({ #value_match_ref }),
                };
            }
        } else {
            quote! {
                let __idx = __labels.iter().position(|l| l == #column_name)
                    .ok_or(::gluesql::core::row_conversion::RowConversionError::MissingColumn { field: #field_name_literal, column: #column_name })?;
                let __v = &__row[__idx];
                if matches!(__v, ::gluesql::core::data::Value::Null) {
                    return Err(::gluesql::core::row_conversion::RowConversionError::NullNotAllowed { field: #field_name_literal, column: __labels[__idx].clone() });
                }
                let #field_ident = { #value_match_ref };
            }
        };

        field_inits.push(init_expr);

        // with precomputed idx
        let idx_pos = field_inits_with_idx.len();
        let init_expr_with_idx = if is_option {
            quote! {
                let __v = &__row[__idx[#idx_pos]];
                let #field_ident = match __v {
                    ::gluesql::core::data::Value::Null => None,
                    __v => Some({ #value_match_idx }),
                };
            }
        } else {
            quote! {
                let __v = &__row[__idx[#idx_pos]];
                if matches!(__v, ::gluesql::core::data::Value::Null) {
                    return Err(::gluesql::core::row_conversion::RowConversionError::NullNotAllowed { field: #field_name_literal, column: __labels[__idx[#idx_pos]].clone() });
                }
                let #field_ident = { #value_match_idx };
            }
        };
        field_inits_with_idx.push(init_expr_with_idx);
    }

    let fields_len = fields_meta_pairs.len();

    let expanded = quote! {
        impl ::gluesql::core::row_conversion::FromGlueRow for #ident {
            fn __glue_fields() -> &'static [(&'static str, &'static str)] {
                static FIELDS: [(&str, &str); #fields_len] = [ #(#fields_meta_pairs),* ];
                &FIELDS
            }
            fn from_glue_row(__labels: &[String], __row: &[::gluesql::core::data::Value]) -> Result<Self, ::gluesql::core::row_conversion::RowConversionError> {
                #(#field_inits)*
                Ok(Self { #(#field_idents),* })
            }
            fn from_glue_row_with_idx(__idx: &[usize], __labels: &[String], __row: &[::gluesql::core::data::Value]) -> Result<Self, ::gluesql::core::row_conversion::RowConversionError> {
                #(#field_inits_with_idx)*
                Ok(Self { #(#field_idents),* })
            }
        }
    };

    Ok(expanded)
}

#[proc_macro_derive(FromGlueRow, attributes(glue))]
pub fn derive_from_glue_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_from_glue_row(input) {
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

fn get_option_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(tp) = ty
        && tp.path.segments.len() == 1
        && tp.path.segments[0].ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &tp.path.segments[0].arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
    }
    None
}

fn is_type(ty: &Type, name: &str) -> bool {
    if let Type::Path(tp) = ty
        && tp.path.segments.len() == 1
    {
        return tp.path.segments[0].ident == name;
    }
    false
}

fn is_string_type(ty: &Type) -> bool {
    if let Type::Path(tp) = ty
        && tp.path.segments.len() == 1
    {
        return tp.path.segments[0].ident == "String";
    }
    false
}

fn last_ident_is(ty: &Type, name: &str) -> bool {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
    {
        return seg.ident == name;
    }
    false
}

fn is_vec_of_u8(ty: &Type) -> bool {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Vec"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return is_type(inner, "u8");
    }
    false
}

fn is_vec_of_value(ty: &Type) -> bool {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Vec"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return last_ident_is(inner, "Value");
    }
    false
}

fn get_vec_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "Vec"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
    }
    None
}

fn is_btreemap_string_value(ty: &Type) -> bool {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "BTreeMap"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
    {
        let mut it = args.args.iter();
        let first = it.next();
        let second = it.next();
        if let (Some(syn::GenericArgument::Type(t1)), Some(syn::GenericArgument::Type(t2))) =
            (first, second)
        {
            return is_string_type(t1) && last_ident_is(t2, "Value");
        }
    }
    false
}

fn get_btreemap_types(ty: &Type) -> Option<(&Type, &Type)> {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == "BTreeMap"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
    {
        let mut it = args.args.iter();
        if let (Some(syn::GenericArgument::Type(k)), Some(syn::GenericArgument::Type(v))) =
            (it.next(), it.next())
        {
            return Some((k, v));
        }
    }
    None
}

fn match_expected(
    base_ty: &Type,
    field_name_literal: &str,
    column_name: &str,
) -> (
    &'static str,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
) {
    // expected_str and match arms for by-label and by-index variants
    let got_str = quote! { match __v {
        ::gluesql::core::data::Value::I8(_) => "I8",
        ::gluesql::core::data::Value::I16(_) => "I16",
        ::gluesql::core::data::Value::I32(_) => "I32",
        ::gluesql::core::data::Value::I64(_) => "I64",
        ::gluesql::core::data::Value::I128(_) => "I128",
        ::gluesql::core::data::Value::U8(_) => "U8",
        ::gluesql::core::data::Value::U16(_) => "U16",
        ::gluesql::core::data::Value::U32(_) => "U32",
        ::gluesql::core::data::Value::U64(_) => "U64",
        ::gluesql::core::data::Value::U128(_) => "U128",
        ::gluesql::core::data::Value::F32(_) => "F32",
        ::gluesql::core::data::Value::F64(_) => "F64",
        ::gluesql::core::data::Value::Decimal(_) => "Decimal",
        ::gluesql::core::data::Value::Bool(_) => "Bool",
        ::gluesql::core::data::Value::Str(_) => "Str",
        ::gluesql::core::data::Value::Bytea(_) => "Bytea",
        ::gluesql::core::data::Value::Inet(_) => "Inet",
        ::gluesql::core::data::Value::Date(_) => "Date",
        ::gluesql::core::data::Value::Timestamp(_) => "Timestamp",
        ::gluesql::core::data::Value::Time(_) => "Time",
        ::gluesql::core::data::Value::Interval(_) => "Interval",
        ::gluesql::core::data::Value::Uuid(_) => "Uuid",
        ::gluesql::core::data::Value::Map(_) => "Map",
        ::gluesql::core::data::Value::List(_) => "List",
        ::gluesql::core::data::Value::Point(_) => "Point",
        ::gluesql::core::data::Value::Null => "Null",
    }};

    macro_rules! arms_copy {
        ($variant:ident, $expected:expr) => {{
            let by_ref = quote! { if let ::gluesql::core::data::Value::$variant(v) = __v { *v } else { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(__labels[__idx].clone()), expected: $expected, got: __got }) } };
            let by_idx = quote! { if let ::gluesql::core::data::Value::$variant(v) = __v { *v } else { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(#column_name.to_string()), expected: $expected, got: __got }) } };
            ($expected, by_ref, by_idx)
        }}
    }

    macro_rules! arms_copy_either {
        ($v1:ident, $v2:ident, $expected:expr) => {{
            let by_ref = quote! {
                if let ::gluesql::core::data::Value::$v1(v) = __v { *v }
                else if let ::gluesql::core::data::Value::$v2(v) = __v { *v }
                else { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(__labels[__idx].clone()), expected: $expected, got: __got }) }
            };
            let by_idx = quote! {
                if let ::gluesql::core::data::Value::$v1(v) = __v { *v }
                else if let ::gluesql::core::data::Value::$v2(v) = __v { *v }
                else { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(#column_name.to_string()), expected: $expected, got: __got }) }
            };
            ($expected, by_ref, by_idx)
        }}
    }

    macro_rules! arms_clone {
        ($variant:ident, $expected:expr) => {{
            let by_ref = quote! { if let ::gluesql::core::data::Value::$variant(v) = __v { v.clone() } else { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(__labels[__idx].clone()), expected: $expected, got: __got }) } };
            let by_idx = quote! { if let ::gluesql::core::data::Value::$variant(v) = __v { v.clone() } else { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(#column_name.to_string()), expected: $expected, got: __got }) } };
            ($expected, by_ref, by_idx)
        }}
    }

    if is_type(base_ty, "i8") {
        return arms_copy!(I8, "i8");
    }
    if is_type(base_ty, "i16") {
        return arms_copy!(I16, "i16");
    }
    if is_type(base_ty, "i32") {
        return arms_copy!(I32, "i32");
    }
    if is_type(base_ty, "i64") {
        return arms_copy!(I64, "i64");
    }
    if is_type(base_ty, "i128") {
        return arms_copy!(I128, "i128");
    }
    if is_type(base_ty, "u8") {
        return arms_copy!(U8, "u8");
    }
    if is_type(base_ty, "u16") {
        return arms_copy!(U16, "u16");
    }
    if is_type(base_ty, "u32") {
        return arms_copy!(U32, "u32");
    }
    if is_type(base_ty, "u64") {
        return arms_copy!(U64, "u64");
    }
    if is_type(base_ty, "u128") {
        return arms_copy_either!(U128, Uuid, "u128");
    }
    if is_type(base_ty, "f32") {
        return arms_copy!(F32, "f32");
    }
    if is_type(base_ty, "f64") {
        return arms_copy!(F64, "f64");
    }
    if is_type(base_ty, "bool") {
        return arms_copy!(Bool, "bool");
    }
    if is_string_type(base_ty) {
        // Accept Value::Str as before, plus Date/Time, and format Timestamp to RFC3339 with trailing 'Z'
        let by_ref = quote! {
            match __v {
                ::gluesql::core::data::Value::Str(v) => v.clone(),
                ::gluesql::core::data::Value::Date(v) => v.to_string(),
                ::gluesql::core::data::Value::Time(v) => v.to_string(),
                ::gluesql::core::data::Value::Timestamp(v) => v.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(),
                _ => { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(__labels[__idx].clone()), expected: "String", got: __got }) }
            }
        };
        let by_idx = quote! {
            match __v {
                ::gluesql::core::data::Value::Str(v) => v.clone(),
                ::gluesql::core::data::Value::Date(v) => v.to_string(),
                ::gluesql::core::data::Value::Time(v) => v.to_string(),
                ::gluesql::core::data::Value::Timestamp(v) => v.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string(),
                _ => { let __got: &str = #got_str; return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch { field: Some(#field_name_literal), column: Some(#column_name.to_string()), expected: "String", got: __got }) }
            }
        };
        return ("String", by_ref, by_idx);
    }
    if last_ident_is(base_ty, "Decimal") {
        return arms_clone!(Decimal, "Decimal");
    }
    if is_vec_of_u8(base_ty) {
        return arms_clone!(Bytea, "Vec<u8>");
    }
    if last_ident_is(base_ty, "IpAddr") {
        return arms_clone!(Inet, "IpAddr");
    }
    if last_ident_is(base_ty, "NaiveDate") {
        return arms_clone!(Date, "NaiveDate");
    }
    if last_ident_is(base_ty, "NaiveDateTime") {
        return arms_clone!(Timestamp, "NaiveDateTime");
    }
    if last_ident_is(base_ty, "NaiveTime") {
        return arms_clone!(Time, "NaiveTime");
    }
    if last_ident_is(base_ty, "Interval") {
        return arms_clone!(Interval, "Interval");
    }
    if is_btreemap_string_value(base_ty) {
        return arms_clone!(Map, "BTreeMap<String, Value>");
    }
    // Support BTreeMap<String, T> from Value::Map by converting each value
    if let Some((k_ty, v_ty)) = get_btreemap_types(base_ty)
        && is_string_type(k_ty)
        && !last_ident_is(v_ty, "Value")
    {
        let expected_str = format!("BTreeMap<String, {}>", quote! { #v_ty });
        let expected_lit = syn::LitStr::new(&expected_str, base_ty.span());

        let val_from_value = if is_string_type(v_ty) {
            quote! { ::std::string::String::from(__item) }
        } else {
            quote! {
                <#v_ty as ::core::convert::TryFrom<&::gluesql::core::data::Value>>::try_from(__item)
                    .map_err(|_| {
                        let __got: &str = #got_str;
                        ::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                            field: Some(#field_name_literal),
                            column: Some(__labels[__idx].clone()),
                            expected: #expected_lit,
                            got: __got,
                        }
                    })?
            }
        };

        let by_ref = quote! {
            if let ::gluesql::core::data::Value::Map(__map) = __v {
                let mut __out: ::std::collections::BTreeMap<::std::string::String, #v_ty> = ::std::collections::BTreeMap::new();
                for (__k, __item) in __map.iter() {
                    let __val: #v_ty = { #val_from_value };
                    __out.insert(__k.clone(), __val);
                }
                __out
            } else {
                let __got: &str = #got_str;
                return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                    field: Some(#field_name_literal),
                    column: Some(__labels[__idx].clone()),
                    expected: #expected_lit,
                    got: __got,
                });
            }
        };

        let val_from_value_idx = if is_string_type(v_ty) {
            quote! { ::std::string::String::from(__item) }
        } else {
            quote! {
                <#v_ty as ::core::convert::TryFrom<&::gluesql::core::data::Value>>::try_from(__item)
                    .map_err(|_| {
                        let __got: &str = #got_str;
                        ::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                            field: Some(#field_name_literal),
                            column: Some(#column_name.to_string()),
                            expected: #expected_lit,
                            got: __got,
                        }
                    })?
            }
        };

        let by_idx = quote! {
            if let ::gluesql::core::data::Value::Map(__map) = __v {
                let mut __out: ::std::collections::BTreeMap<::std::string::String, #v_ty> = ::std::collections::BTreeMap::new();
                for (__k, __item) in __map.iter() {
                    let __val: #v_ty = { #val_from_value_idx };
                    __out.insert(__k.clone(), __val);
                }
                __out
            } else {
                let __got: &str = #got_str;
                return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                    field: Some(#field_name_literal),
                    column: Some(#column_name.to_string()),
                    expected: #expected_lit,
                    got: __got,
                });
            }
        };

        return ("BTreeMap<_, _>", by_ref, by_idx);
    }
    if is_vec_of_value(base_ty) {
        return arms_clone!(List, "Vec<Value>");
    }
    // Support Vec<T> from Value::List by converting each element
    if let Some(inner) = get_vec_inner_type(base_ty)
        && !is_type(inner, "u8")
        && !last_ident_is(inner, "Value")
    {
        let expected_str = format!("Vec<{}>", quote! { #inner });
        let expected_lit = syn::LitStr::new(&expected_str, base_ty.span());

        // String inner uses `String::from(&Value)`; others use `TryFrom<&Value>`
        let elem_from_value = if is_string_type(inner) {
            quote! { ::std::string::String::from(__item) }
        } else {
            quote! {
                <#inner as ::core::convert::TryFrom<&::gluesql::core::data::Value>>::try_from(__item)
                    .map_err(|_| {
                        let __got: &str = #got_str;
                        ::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                            field: Some(#field_name_literal),
                            column: Some(__labels[__idx].clone()),
                            expected: #expected_lit,
                            got: __got,
                        }
                    })?
            }
        };

        let by_ref = quote! {
            if let ::gluesql::core::data::Value::List(__list) = __v {
                let mut __out: ::std::vec::Vec<#inner> = ::std::vec::Vec::with_capacity(__list.len());
                for __item in __list.iter() {
                    let __elem: #inner = { #elem_from_value };
                    __out.push(__elem);
                }
                __out
            } else {
                let __got: &str = #got_str;
                return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                    field: Some(#field_name_literal),
                    column: Some(__labels[__idx].clone()),
                    expected: #expected_lit,
                    got: __got,
                });
            }
        };

        let elem_from_value_idx = if is_string_type(inner) {
            quote! { ::std::string::String::from(__item) }
        } else {
            quote! {
                <#inner as ::core::convert::TryFrom<&::gluesql::core::data::Value>>::try_from(__item)
                    .map_err(|_| {
                        let __got: &str = #got_str;
                        ::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                            field: Some(#field_name_literal),
                            column: Some(#column_name.to_string()),
                            expected: #expected_lit,
                            got: __got,
                        }
                    })?
            }
        };

        let by_idx = quote! {
            if let ::gluesql::core::data::Value::List(__list) = __v {
                let mut __out: ::std::vec::Vec<#inner> = ::std::vec::Vec::with_capacity(__list.len());
                for __item in __list.iter() {
                    let __elem: #inner = { #elem_from_value_idx };
                    __out.push(__elem);
                }
                __out
            } else {
                let __got: &str = #got_str;
                return Err(::gluesql::core::row_conversion::RowConversionError::TypeMismatch {
                    field: Some(#field_name_literal),
                    column: Some(#column_name.to_string()),
                    expected: #expected_lit,
                    got: __got,
                });
            }
        };

        return ("Vec<_>", by_ref, by_idx);
    }
    if last_ident_is(base_ty, "Point") {
        return arms_clone!(Point, "Point");
    }

    let msg = format!(
        "Unsupported field type for FromGlueRow: `{}`. Supported: direct matches of Value variants (integers, floats, bool, String, Decimal, Vec<u8>, IpAddr, chrono NaiveDate/NaiveDateTime/NaiveTime, Interval, u128 for Uuid, BTreeMap<String, Value>, Vec<Value>, Point) and Option<T> of those.",
        quote! { #base_ty }
    );
    (
        "<unsupported>",
        syn::Error::new(base_ty.span(), &msg).to_compile_error(),
        syn::Error::new(base_ty.span(), &msg).to_compile_error(),
    )
}

#[cfg(test)]
mod tests {
    use super::expand_from_glue_row;
    use syn::parse_quote;

    #[test]
    fn non_struct_input_returns_error() {
        let di: syn::DeriveInput = parse_quote! {
            enum E { A }
        };
        let err = expand_from_glue_row(di).unwrap_err();
        assert!(
            err.to_string()
                .contains("FromGlueRow can only be derived for structs")
        );
    }

    #[test]
    fn non_named_fields_struct_returns_error() {
        let di: syn::DeriveInput = parse_quote! {
            struct T(i32);
        };
        let err = expand_from_glue_row(di).unwrap_err();
        assert!(
            err.to_string()
                .contains("FromGlueRow supports only named fields")
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
        let _ = expand_from_glue_row(di).expect("expand ok");
    }

    #[test]
    fn glue_rename_parse_error_missing_args() {
        let di: syn::DeriveInput = parse_quote! {
            struct S { #[glue] a: i64 }
        };
        let err = expand_from_glue_row(di).unwrap_err();
        let s = err.to_string();
        assert!(!s.is_empty(), "unexpected empty error message");
    }

    #[test]
    fn glue_rename_wrong_literal_type() {
        let di: syn::DeriveInput = parse_quote! {
            struct S { #[glue(rename = 123)] a: i64 }
        };
        let err = expand_from_glue_row(di).unwrap_err();
        assert!(
            err.to_string()
                .contains("expected string literal for rename")
        );
    }

    #[test]
    fn match_expected_all_types_and_options_expand() {
        let di: syn::DeriveInput = parse_quote! {
            struct All {
                i8_: i8,
                i16_: i16,
                i32_: i32,
                i64_: i64,
                i128_: i128,
                u8_: u8,
                u16_: u16,
                u32_: u32,
                u64_: u64,
                u128_: u128,
                f32_: f32,
                f64_: f64,
                b_: bool,
                s_: String,
                dec_: Decimal,
                bytes_: Vec<u8>,
                ip_: IpAddr,
                date_: NaiveDate,
                ts_: NaiveDateTime,
                time_: NaiveTime,
                interval_: Interval,
                map_: BTreeMap<String, Value>,
                list_: Vec<Value>,
                point_: Point,
                opt_s: Option<String>,
                opt_i64: Option<i64>,
            }
        };
        let _ = expand_from_glue_row(di).expect("expand ok");
    }

    #[test]
    fn unsupported_type_path_emits_compile_error_tokens() {
        let di: syn::DeriveInput = parse_quote! {
            struct S<'a> { v: &'a str }
        };
        let ts = expand_from_glue_row(di).expect("expand ok");
        let s = ts.to_string();
        assert!(s.contains("Unsupported field type for FromGlueRow"));
    }
}
