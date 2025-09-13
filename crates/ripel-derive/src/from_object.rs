use quote::quote;
use crate::opts::FieldOpts;
use crate::util::option_inner_ty;

pub fn gen_from_object_assign(
    entity_ident: &syn::Ident,
    f: &FieldOpts,
) -> proc_macro2::TokenStream {
    let ident_f  = f.ident.as_ref().unwrap();

    let col_name = f.column.clone().unwrap_or_else(|| ident_f.to_string());
    let col_lit  = lit(&col_name);

    let inner_ty_opt = option_inner_ty(&f.ty);
    let is_option    = inner_ty_opt.is_some();
    let inner_ty     = inner_ty_opt.unwrap_or(&f.ty);

    // Common strings we’ll embed in the message
    let field_name_msg  = format!("`{}`", ident_f);
    let entity_name_msg = format!("{}", entity_ident);
    let rust_ty_msg     = format!("{}", quote!(#inner_ty));

    if let Some(tpl) = &f.template {
        let tpl_lit = lit(tpl);

        if is_option {
            // Optional + Template
            quote! {
                #ident_f: {
                    #[allow(unused_imports)]
                    use ::anyhow::Context as _;
                    let __dv = ::ripel::core::interpolate::eval_template(obj, env, #tpl_lit)
                        .with_context(|| format!(
                            "interpolating field {} of {} (template: {})",
                            #field_name_msg, #entity_name_msg, #tpl_lit
                        ))?;
                    if __dv.is_none() {
                        None
                    } else {
                        Some(<#inner_ty>::try_from(__dv.clone()).with_context(|| format!(
                            "coercing template result for field {} of {} to {} ; got {:?}",
                            #field_name_msg, #entity_name_msg, #rust_ty_msg, __dv
                        ))?)
                    }
                }
            }
        } else {
            // Required + Template
            quote! {
                #ident_f: {
                    #[allow(unused_imports)]
                    use ::anyhow::{anyhow, Context as _};
                    let __dv = ::ripel::core::interpolate::eval_template(obj, env, #tpl_lit)
                        .with_context(|| format!(
                            "interpolating field {} of {} (template: {})",
                            #field_name_msg, #entity_name_msg, #tpl_lit
                        ))?;
                    if __dv.is_none() {
                        return Err(anyhow!(
                            "template produced NULL for field {} of {} (expected {})",
                            #field_name_msg, #entity_name_msg, #rust_ty_msg
                        ));
                    }
                    <#inner_ty>::try_from(__dv.clone()).with_context(|| format!(
                        "coercing template result for field {} of {} to {} ; got {:?}",
                        #field_name_msg, #entity_name_msg, #rust_ty_msg, __dv
                    ))?
                }
            }
        }
    } else {
        if is_option {
            // Optional + Column
            quote! {
                #ident_f: {
                    #[allow(unused_imports)]
                    use ::anyhow::Context as _;
                    let __dv = ::ripel::core::interpolate::get_col(obj, #col_lit)
                        .with_context(|| format!(
                            "reading column {} into field {} of {}",
                            #col_lit, #field_name_msg, #entity_name_msg
                        ))?;
                    if __dv.is_none() {
                        None
                    } else {
                        Some(<#inner_ty>::try_from(__dv.clone()).with_context(|| format!(
                            "coercing column {} for field {} of {} to {} ; got {:?}",
                            #col_lit, #field_name_msg, #entity_name_msg, #rust_ty_msg, __dv
                        ))?)
                    }
                }
            }
        } else {
            // Required + Column
            quote! {
                #ident_f: {
                    #[allow(unused_imports)]
                    use ::anyhow::{anyhow, Context as _};
                    let __dv = ::ripel::core::interpolate::get_col(obj, #col_lit)
                        .with_context(|| format!(
                            "reading column {} into field {} of {}",
                            #col_lit, #field_name_msg, #entity_name_msg
                        ))?;
                    if __dv.is_none() {
                        return Err(anyhow!(
                            "column {} is NULL but field {} of {} is not optional (expected {})",
                            #col_lit, #field_name_msg, #entity_name_msg, #rust_ty_msg
                        ));
                    }
                    <#inner_ty>::try_from(__dv.clone()).with_context(|| format!(
                        "coercing column {} for field {} of {} to {} ; got {:?}",
                        #col_lit, #field_name_msg, #entity_name_msg, #rust_ty_msg, __dv
                    ))?
                }
            }
        }
    }
}

fn lit(s: &str) -> syn::LitStr {
    syn::LitStr::new(s, proc_macro2::Span::call_site())
}
