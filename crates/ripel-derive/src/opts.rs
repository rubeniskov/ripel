use darling::{FromDeriveInput, FromField, ast};

#[derive(Debug, FromField)]
#[darling(attributes(ripel))]
pub struct FieldOpts {
    pub ident: Option<syn::Ident>,
    pub ty: syn::Type,

    #[darling(default)]
    pub primary_key: bool,

    #[darling(default)]
    pub column: Option<String>,

    #[darling(default)]
    pub reference: Option<String>,

    #[darling(default)]
    pub template: Option<String>,

    #[darling(default)]
    pub via: Option<String>,
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(ripel), supports(struct_named))]
pub struct EntityOpts {
    pub ident: syn::Ident,
    pub generics: syn::Generics,

    #[darling(default)]
    pub name: Option<String>,

    pub table_name: String,

    pub data: ast::Data<darling::util::Ignored, FieldOpts>,
}
