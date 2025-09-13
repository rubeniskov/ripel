pub fn option_inner_ty(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::Path(tp) = ty {
        let seg = tp.path.segments.last()?;
        if seg.ident == "Option" {
            if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                for ga in &args.args {
                    if let syn::GenericArgument::Type(inner) = ga {
                        return Some(inner);
                    }
                }
            }
        }
    }
    None
}
