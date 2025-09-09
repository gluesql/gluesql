use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, Fields, Lit, MetaNameValue, Type,
    parse_macro_input, spanned::Spanned,
};

#[proc_macro_derive(FromGlueRow, attributes(glue))]
pub fn derive_from_glue_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let input_span = input.span();

    let ident = input.ident.clone();
    let data = match input.data {
        Data::Struct(s) => s,
        _ => {
            return syn::Error::new(input_span, "FromGlueRow can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    let fields = match data.fields {
        Fields::Named(f) => f.named,
        _ => {
            return syn::Error::new(input_span, "FromGlueRow supports only named fields")
                .to_compile_error()
                .into();
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
                    Err(e) => return e.to_compile_error().into(),
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

    TokenStream::from(expanded)
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
        return arms_clone!(Str, "String");
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
    if is_vec_of_value(base_ty) {
        return arms_clone!(List, "Vec<Value>");
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
