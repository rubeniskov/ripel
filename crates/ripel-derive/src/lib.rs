use proc_macro::TokenStream;
use syn::parse_macro_input;
use syn::DeriveInput;

mod opts;
mod util;
mod parse;
mod codegen;
mod from_object;
mod parts;

#[proc_macro_derive(Entity, attributes(ripel))]
pub fn derive_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match codegen::expand(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.write_errors().into(),
    }
}
