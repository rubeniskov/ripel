use darling::FromDeriveInput;
use quote::{format_ident, quote};
use syn::DeriveInput;

use crate::opts::EntityOpts;
use crate::parts::*;
use crate::from_object::gen_from_object_assign;

pub fn expand(input: DeriveInput) -> Result<proc_macro2::TokenStream, darling::Error> {
    let opts = EntityOpts::from_derive_input(&input)?;

    // guard: no generics
    if !opts.generics.params.is_empty() {
        return Err(darling::Error::unsupported_shape("generic type parameters are not supported"));
    }

    let ident = &opts.ident;

    // names & literals
    let rust_name = ident.to_string();
    let entity_name_lit = lit(opts.name.as_deref().unwrap_or(&rust_name));
    let table_name_lit = lit(&opts.table_name);

    let fields = match &opts.data {
        darling::ast::Data::Struct(s) => s,
        _ => unreachable!("supports(struct_named) enforces named fields"),
    };

    // Parts collectors
    let mut table_field_inits = Vec::new();
    let mut ref_field_inits   = Vec::new();
    let mut via_consts        = Vec::new();
    let mut from_object_fields= Vec::new();

    // Track PK
    let mut pk_name: Option<String> = None;

    for f in fields.iter() {
        // table field
        table_field_inits.push(gen_table_field(f));

        // reference field (and maybe VIA const)
        if has_reference(f) {
            let (via_const_opt, ref_field) = gen_reference_field(ident, f)?;
            if let Some(c) = via_const_opt { via_consts.push(c); }
            ref_field_inits.push(ref_field);
        }

        // FromObject assignment
        from_object_fields.push(gen_from_object_assign(ident, f));

        // PK check
        if f.primary_key {
            if pk_name.is_some() {
                return Err(darling::Error::custom("Multiple fields marked `#[ripel(primary_key)]`").with_span(&f.ident));
            }
            pk_name = Some(f.ident.as_ref().unwrap().to_string());
        }
    }

    let pk = pk_name.ok_or_else(|| {
        darling::Error::custom("No field marked `#[ripel(primary_key)]`").with_span(&opts.ident)
    })?;
    let pk_lit = lit(&pk);

    // symbols
    let fields_sym = format_ident!("__{}_FIELDS", rust_name.to_uppercase());
    let model_sym  = format_ident!("__{}_MODEL",  rust_name.to_uppercase());
    let reg_sym    = format_ident!("__RIPEL_ENTITY_ENTRY_{}", rust_name.to_uppercase());

    // final assembly
    let ts = quote! {
        #( #via_consts )*

        const #fields_sym: &[::ripel::core::entity::FieldModel] = &[
            #( #table_field_inits ),*,
            #( #ref_field_inits ),*
        ];

        const #model_sym: ::ripel::core::entity::EntityModel =
            ::ripel::core::entity::EntityModel {
                entity_name: #entity_name_lit,
                table_name:  #table_name_lit,
                rust_name:   stringify!(#ident),
                fields:      #fields_sym,
                primary_key: #pk_lit,
            };

        #[automatically_derived]
        impl ::ripel::core::entity::Entity for #ident {
            const MODEL: &'static ::ripel::core::entity::EntityModel = &#model_sym;
        }

        #[automatically_derived]
        impl ::ripel::core::interpolate::FromObject for #ident {
            fn from_object<T: ::ripel::core::Object + Clone + 'static>(
                obj: &T,
                env: &minijinja::Environment<'_>
            ) -> ::anyhow::Result<Self> {
                Ok(Self { #( #from_object_fields, )* })
            }
        }

        #[automatically_derived]
        #[used]
        #[cfg_attr(any(target_os = "linux", target_os = "android"), unsafe(link_section = ".ripel_entities$m"))]
        #[cfg_attr(target_os = "macos", unsafe(link_section = "__DATA,__ripel_entities"))]
        #[cfg_attr(windows, unsafe(link_section = ".ripel_entities$m"))]
        static #reg_sym: ::ripel::core::registry::Entry =
            ::ripel::core::registry::Entry(|| { <#ident as ::ripel::core::entity::Entity>::MODEL });
    };

    Ok(ts)
}

// small sugar
fn lit(s: &str) -> syn::LitStr {
    syn::LitStr::new(s, proc_macro2::Span::call_site())
}
