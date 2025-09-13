use proc_macro2::Span;
use quote::{format_ident, quote};
use crate::opts::FieldOpts;
use crate::parse::parse_via_literal;
use crate::util::option_inner_ty;

pub fn has_reference(f: &FieldOpts) -> bool {
    f.reference.is_some()
}

pub fn gen_table_field(f: &FieldOpts) -> proc_macro2::TokenStream {
    let fid      = f.ident.as_ref().expect("named field");
    let name     = fid.to_string();
    let name_lit = lit(&name);

    let col_name = f.column.clone().unwrap_or_else(|| name.clone());
    let col_lit  = lit(&col_name);

    let inner_ty  = option_inner_ty(&f.ty).unwrap_or(&f.ty);
    let ty_name   = quote! { stringify!(#inner_ty) };

    let primary_lit  = syn::LitBool::new(f.primary_key, Span::call_site());
    let nullable_lit = syn::LitBool::new(option_inner_ty(&f.ty).is_some(), Span::call_site());

    let tmpl = if let Some(t) = &f.template {
        let t_lit = lit(t);
        quote! { Some(#t_lit) }
    } else {
        quote! { None }
    };

    quote! {
        ::ripel::core::entity::FieldModel::TableField(
            ::ripel::core::entity::TableField {
                name: #name_lit,
                primary_key: #primary_lit,
                column: #col_lit,
                template: #tmpl,
                ty_name: #ty_name,
                nullable: #nullable_lit,
            }
        )
    }
}

pub fn gen_reference_field(
    entity_ident: &syn::Ident,
    f: &FieldOpts,
) -> Result<(Option<proc_macro2::TokenStream>, proc_macro2::TokenStream), darling::Error> {
    let fid      = f.ident.as_ref().expect("named field");
    let name     = fid.to_string();
    let name_lit = lit(&name);

    let inner_ty  = option_inner_ty(&f.ty).unwrap_or(&f.ty);
    let ty_name   = quote! { stringify!(#inner_ty) };

    let reference_str = f.reference.as_ref().unwrap();
    let reference_lit = lit(reference_str);

    let via_ident = format_ident!(
        "__{}_{}_VIA",
        entity_ident.to_string().to_uppercase(),
        name.to_uppercase()
    );

    let (via_const, via_tokens) = if let Some(via) = &f.via {
        let hops = parse_via_literal(via).map_err(|e|
            darling::Error::custom(format!("Error parsing `via`: {e}")).with_span(fid)
        )?;

        let hop_inits: Vec<_> = hops.iter().map(|(table, lhs, rhs)| {
            let tl = lit(table);
            let ll = lit(lhs);
            let rl = lit(rhs);
            quote! { ::ripel::core::refs::Hop { table: #tl, lhs: #ll, rhs: #rl } }
        }).collect();

        let via_const = quote! {
            const #via_ident: &[::ripel::core::refs::Hop] = &[ #( #hop_inits ),* ];
        };

        (Some(via_const), quote! { #via_ident })
    } else {
        (None, quote! { &[] })
    };

    let ref_field = quote! {
        ::ripel::core::entity::FieldModel::ReferenceField(
            ::ripel::core::entity::ReferenceField {
                name: #name_lit,
                reference: #reference_lit,
                via: #via_tokens,
                ty_name: #ty_name
            }
        )
    };

    Ok((via_const, ref_field))
}

fn lit(s: &str) -> syn::LitStr {
    syn::LitStr::new(s, Span::call_site())
}
