pub fn parse_via_literal(s: &str) -> Result<Vec<(&str, &str, &str)>, syn::Error> {
    let mut out = Vec::new();
    for raw in s.split("->") {
        let (table, lhs, rhs) = ripel_core::refs::parse_hop_literal(raw.trim())
            .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Error parsing `via`: {e}")))?;
        out.push((table, lhs, rhs));
    }
    if out.is_empty() {
        return Err(syn::Error::new(proc_macro2::Span::call_site(), "empty via chain"));
    }
    Ok(out)
}
