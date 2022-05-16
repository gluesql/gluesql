use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_attribute]
pub fn basic_derives(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = quote!(
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #input
    );
    TokenStream::from(expanded)
}
